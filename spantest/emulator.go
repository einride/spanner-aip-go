package spantest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/spansql"
	"google.golang.org/api/option"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
	"google.golang.org/grpc"
	"gotest.tools/v3/assert"
)

// EmulatorDockerFixture is a test fixture running the Spanner emulator.
type EmulatorDockerFixture struct {
	Ctx                 context.Context
	Conn                *grpc.ClientConn
	InstanceAdminClient *instance.InstanceAdminClient
	DatabaseAdminClient *database.DatabaseAdminClient
	EmulatorHost        string
	ProjectID           string
	InstanceID          string
}

// NewEmulatorDockerFixture creates a test fixture for a containerized Spanner emulator.
func NewEmulatorDockerFixture(t *testing.T) *EmulatorDockerFixture {
	t.Helper()
	if !HasDocker() {
		t.Fatal("No Docker client available for running the Spanner emulator container.")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if deadline, ok := t.Deadline(); ok {
		ctx, cancel = context.WithDeadline(ctx, deadline)
		t.Cleanup(cancel)
	}
	const cloudSpannerEmulatorImage = "gcr.io/cloud-spanner-emulator/emulator:latest"
	dockerPull(t, cloudSpannerEmulatorImage)
	var containerID string
	if isRunningOnCloudBuild(t) {
		containerID = dockerRunDetached(t, "--network", "cloudbuild", "--publish-all", cloudSpannerEmulatorImage)
	} else {
		containerID = dockerRunDetached(t, "--publish-all", cloudSpannerEmulatorImage)
	}
	t.Cleanup(func() {
		t.Log(dockerLogs(t, containerID))
		dockerKill(t, containerID)
		dockerRm(t, containerID)
	})
	emulatorHost := inspectPortAddress(t, containerID, "9010/tcp")
	t.Log("emulator host:", emulatorHost)
	awaitReachable(t, emulatorHost, 1*time.Second, 10*time.Second)
	conn, err := grpc.Dial(emulatorHost, grpc.WithInsecure())
	assert.NilError(t, err)
	t.Cleanup(func() {
		assert.NilError(t, conn.Close())
	})
	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx, option.WithGRPCConn(conn))
	assert.NilError(t, err)
	t.Log("creating instance...")
	const (
		projectID  = "spanner-aip-go"
		instanceID = "emulator"
	)
	createInstanceOp, err := instanceAdminClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectID),
		InstanceId: "emulator",
		Instance: &instancepb.Instance{
			DisplayName: "Emulator",
			NodeCount:   1,
		},
	})
	assert.NilError(t, err)
	createdInstance, err := createInstanceOp.Wait(ctx)
	assert.NilError(t, err)
	t.Log("instance:", createdInstance.String())
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx, option.WithGRPCConn(conn))
	assert.NilError(t, err)
	return &EmulatorDockerFixture{
		Ctx:                 ctx,
		Conn:                conn,
		InstanceAdminClient: instanceAdminClient,
		DatabaseAdminClient: databaseAdminClient,
		ProjectID:           projectID,
		InstanceID:          instanceID,
		EmulatorHost:        emulatorHost,
	}
}

// NewDatabaseFromDDLFiles creates a new database with a random ID from the provided DDL file path glob.
func (fx *EmulatorDockerFixture) NewDatabaseFromDDLFiles(t *testing.T, glob string) *spanner.Client {
	t.Helper()
	files, err := filepath.Glob(glob)
	assert.NilError(t, err)
	var statements []string
	for _, file := range files {
		content, err := ioutil.ReadFile(file)
		assert.NilError(t, err)
		ddl, err := spansql.ParseDDL(file, string(content))
		assert.NilError(t, err)
		for _, ddlStmt := range ddl.List {
			statements = append(statements, ddlStmt.SQL())
		}
	}
	assert.Assert(t, len(statements) > 0)
	return fx.NewDatabaseFromStatements(t, statements)
}

// NewDatabaseFromDDLFiles creates a new database with a random ID from the provided statements.
func (fx *EmulatorDockerFixture) NewDatabaseFromStatements(t *testing.T, statements []string) *spanner.Client {
	t.Helper()
	databaseID := "db" + strconv.Itoa(rand.Int()) // nolint: gosec
	createDatabaseOp, err := fx.DatabaseAdminClient.CreateDatabase(fx.Ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", fx.ProjectID, fx.InstanceID),
		CreateStatement: fmt.Sprintf("CREATE DATABASE %s", databaseID),
		ExtraStatements: statements,
	})
	assert.NilError(t, err)
	createdDatabase, err := createDatabaseOp.Wait(fx.Ctx)
	assert.NilError(t, err)
	t.Log("database:", createdDatabase.String())
	conn, err := grpc.Dial(fx.EmulatorHost, grpc.WithInsecure())
	assert.NilError(t, err)
	client, err := spanner.NewClient(fx.Ctx, createdDatabase.Name, option.WithGRPCConn(conn))
	assert.NilError(t, err)
	t.Cleanup(client.Close)
	return client
}

// HasDocker returns true if Docker is available on the local host.
func HasDocker() bool {
	_, err := exec.LookPath("docker")
	return err == nil
}

func dockerPull(t *testing.T, image string) {
	t.Helper()
	execCommand(t, "docker", "pull", image)
}

func dockerRm(t *testing.T, containerID string) {
	t.Helper()
	execCommand(t, "docker", "rm", "-v", containerID)
}

func dockerKill(t *testing.T, containerID string) {
	t.Helper()
	execCommand(t, "docker", "kill", containerID)
}

func inspectPortAddress(t *testing.T, containerID string, containerPort string) string {
	t.Helper()
	var containers []struct {
		NetworkSettings struct {
			Ports map[string][]struct {
				HostIP   string
				HostPort string
			}
			Networks map[string]struct {
				Gateway string
			}
		}
	}
	stdout := execCommand(t, "docker", "inspect", containerID)
	assert.NilError(t, json.NewDecoder(strings.NewReader(stdout)).Decode(&containers))
	var host string
	var port string
	for _, container := range containers {
		for portID, hostPorts := range container.NetworkSettings.Ports {
			if portID == containerPort {
				for _, hostPort := range hostPorts {
					host, port = hostPort.HostIP, hostPort.HostPort
				}
			}
		}
		for networkID, network := range container.NetworkSettings.Networks {
			if networkID == "cloudbuild" {
				host = network.Gateway
			}
		}
	}
	if host == "" || port == "" {
		t.Fatalf("failed to inspect container %s for port %s", containerID, containerPort)
	}
	return fmt.Sprintf("%s:%s", host, port)
}

func execCommand(t *testing.T, name string, args ...string) string {
	t.Helper()
	t.Log("exec:", name, strings.Join(args, " "))
	cmd := exec.Command(name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	assert.NilError(t, cmd.Run(), stderr.String())
	return strings.TrimSpace(stdout.String())
}

func dockerRunDetached(t *testing.T, args ...string) string {
	t.Helper()
	stdout := execCommand(t, "docker", append([]string{"run", "-d"}, args...)...)
	containerID := strings.TrimSpace(stdout)
	assert.Assert(t, containerID != "")
	t.Log("id:", containerID)
	return containerID
}

func awaitReachable(t *testing.T, addr string, wait, maxWait time.Duration) {
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

func dockerLogs(t *testing.T, containerID string) string {
	t.Helper()
	t.Log("exec:", "docker", "logs", containerID)
	cmd := exec.Command("docker", "logs", containerID)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	assert.NilError(t, cmd.Run(), stderr.String())
	return strings.TrimSpace(stderr.String())
}

func isRunningOnCloudBuild(t *testing.T) bool {
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
