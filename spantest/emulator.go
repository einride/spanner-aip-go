package spantest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base32"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gotest.tools/v3/assert"
)

// Cloud Spanner Emulator versions can be found here,
// https://console.cloud.google.com/gcr/images/cloud-spanner-emulator/global/emulator
const (
	url     = "gcr.io/cloud-spanner-emulator/emulator"
	version = "1.5.29"
	image   = url + ":" + version
)

// EmulatorFixture is a test fixture running the Spanner emulator.
type EmulatorFixture struct {
	ctx                 context.Context
	conn                *grpc.ClientConn
	instanceAdminClient *instance.InstanceAdminClient
	databaseAdminClient *database.DatabaseAdminClient
	emulatorHost        string
	projectID           string
	instanceID          string
}

// NewEmulatorFixture creates a test fixture for a containerized Spanner emulator.
func NewEmulatorFixture(t testing.TB) Fixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if tt, ok := t.(*testing.T); ok {
		if deadline, ok := tt.Deadline(); ok {
			ctx, cancel = context.WithDeadline(ctx, deadline)
			t.Cleanup(cancel)
		}
	}
	emulatorHost, ok := os.LookupEnv("SPANNER_EMULATOR_HOST")
	if !ok {
		if !HasDocker() {
			t.Fatal("No Docker client available for running the Spanner emulator container.")
		}
		if !IsDockerDaemonRunning() {
			t.Fatal("Docker is available, but the daemon does not seem to be running.")
		}
		var containerID string
		if isRunningOnCloudBuild(t) {
			containerID = dockerRunDetached(t, "--network", "cloudbuild", "--publish-all", image)
		} else {
			containerID = dockerRunDetached(t, "--publish-all", image)
		}
		t.Cleanup(func() {
			t.Log(dockerLogs(t, containerID))
			dockerKill(t, containerID)
			dockerRm(t, containerID)
		})
		emulatorHost = inspectPortAddress(t, containerID, "9010/tcp")
		t.Log("using emulator from container config", emulatorHost)
	} else {
		t.Log("using emulator from environment")
	}
	t.Log("emulator host:", emulatorHost)
	awaitReachable(t, emulatorHost, 100*time.Millisecond, 10*time.Second)
	conn, err := grpc.NewClient(
		emulatorHost,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	assert.NilError(t, err)
	t.Cleanup(func() {
		assert.NilError(t, conn.Close())
	})
	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx, option.WithGRPCConn(conn))
	assert.NilError(t, err)
	t.Log("creating instance...")
	const projectID = "spanner-aip-go"
	instanceID := fmt.Sprintf("emulator-%s", randomSuffix(t))
	createInstanceOp, err := instanceAdminClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectID),
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			DisplayName: "Emulator",
			NodeCount:   1,
		},
	})
	assert.NilError(t, err, "failed to create instance")
	createdInstance, err := createInstanceOp.Wait(ctx)
	assert.NilError(t, err, "failed to prepare instance for serving")
	t.Log("instance:", createdInstance.String())
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx, option.WithGRPCConn(conn))
	assert.NilError(t, err, "failed to create database admin client")
	return &EmulatorFixture{
		ctx:                 ctx,
		conn:                conn,
		instanceAdminClient: instanceAdminClient,
		databaseAdminClient: databaseAdminClient,
		projectID:           projectID,
		instanceID:          instanceID,
		emulatorHost:        emulatorHost,
	}
}

func (fx *EmulatorFixture) Context() context.Context {
	return fx.ctx
}

// NewDatabase creates a new database with a random ID based on the passed options.
func (fx *EmulatorFixture) NewDatabase(t testing.TB, options ...DatabaseCreationOption) *spanner.Client {
	t.Helper()
	cfg := getDatabaseCreationConfig(t, options...)
	var protoDescriptors []byte
	if cfg.protoDescriptorReader != nil {
		var err error
		protoDescriptors, err = io.ReadAll(cfg.protoDescriptorReader)
		assert.NilError(t, err)
	}
	databaseID := fmt.Sprintf("db%s", randomSuffix(t))
	createDatabaseOp, err := fx.databaseAdminClient.CreateDatabase(fx.ctx, &databasepb.CreateDatabaseRequest{
		Parent:           fmt.Sprintf("projects/%s/instances/%s", fx.projectID, fx.instanceID),
		CreateStatement:  fmt.Sprintf("CREATE DATABASE %s", databaseID),
		ExtraStatements:  cfg.statements,
		ProtoDescriptors: protoDescriptors,
	})
	assert.NilError(t, err)
	createdDatabase, err := createDatabaseOp.Wait(fx.ctx)
	assert.NilError(t, err)
	t.Log("database:", createdDatabase.String())
	conn, err := grpc.NewClient(fx.emulatorHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NilError(t, err)
	client, err := spanner.NewClient(fx.ctx, createdDatabase.GetName(), option.WithGRPCConn(conn))
	assert.NilError(t, err)
	t.Cleanup(client.Close)
	return client
}

// NewDatabaseFromDDLFiles creates a new database with a random ID from the provided DDL file path glob.
func (fx *EmulatorFixture) NewDatabaseFromDDLFiles(t testing.TB, globs ...string) *spanner.Client {
	return fx.NewDatabase(t, FromGlobs(globs...))
}

// NewDatabaseFromStatements creates a new database with a random ID from the provided statements.
func (fx *EmulatorFixture) NewDatabaseFromStatements(t testing.TB, statements []string) *spanner.Client {
	return fx.NewDatabase(t, FromStatements(statements))
}

// HasDocker returns true if Docker is available on the local host.
func HasDocker() bool {
	_, err := exec.LookPath("docker")
	return err == nil
}

// IsDockerDaemonRunning reports if the Docker daemon is running.
func IsDockerDaemonRunning() bool {
	return exec.Command("docker", "info").Run() == nil
}

func dockerRm(t testing.TB, containerID string) {
	t.Helper()
	execCommand(t, "docker", "rm", "-v", containerID)
}

func dockerKill(t testing.TB, containerID string) {
	t.Helper()
	execCommand(t, "docker", "kill", containerID)
}

func inspectPortAddress(t testing.TB, containerID, containerPort string) string {
	t.Helper()
	output := execCommand(t, "docker", "port", containerID, containerPort)
	lines := strings.Split(output, "\n")
	// docker port can return ipv6 mapping as well, take the first non ipv6 mapping.
	for _, line := range lines {
		mapping := strings.TrimSpace(line)
		if _, err := net.ResolveTCPAddr("tcp4", mapping); err == nil {
			return mapping
		}
	}
	return ""
}

func execCommand(t testing.TB, name string, args ...string) string {
	t.Helper()
	t.Log("exec:", name, strings.Join(args, " "))
	cmd := exec.Command(name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	assert.NilError(t, cmd.Run(), stderr.String())
	return strings.TrimSpace(stdout.String())
}

func dockerRunDetached(t testing.TB, args ...string) string {
	t.Helper()
	stdout := execCommand(t, "docker", append([]string{"run", "-d"}, args...)...)
	containerID := strings.TrimSpace(stdout)
	assert.Assert(t, containerID != "")
	t.Log("id:", containerID)
	return containerID
}

func awaitReachable(t testing.TB, addr string, wait, maxWait time.Duration) {
	t.Helper()
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		if c, err := net.Dial("tcp", addr); err == nil {
			_ = c.Close()
			return
		}
		t.Logf("failed to reach %s, sleeping for %v", addr, wait)
		time.Sleep(wait)
	}
	t.Fatalf("%v unreachable for %v", addr, maxWait)
}

func dockerLogs(t testing.TB, containerID string) string {
	t.Helper()
	t.Log("exec:", "docker", "logs", containerID)
	cmd := exec.Command("docker", "logs", containerID)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	assert.NilError(t, cmd.Run(), stderr.String())
	return strings.TrimSpace(stderr.String())
}

func isRunningOnCloudBuild(t testing.TB) bool {
	t.Helper()
	t.Log("exec:", "docker", "network", "inspect", "cloudbuild")
	cmd := exec.Command("docker", "network", "inspect", "cloudbuild")
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	result := cmd.Run() == nil
	if result {
		t.Log(stdout.String())
	}
	return result
}

func randomSuffix(t testing.TB) string {
	data := make([]byte, 10)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	return strings.ToLower(base32.HexEncoding.WithPadding(base32.NoPadding).EncodeToString(data))
}
