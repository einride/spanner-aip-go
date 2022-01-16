//go:build mage
// +build mage

package main

import (
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
	"go.einride.tech/mage-tools/mgmake"
	"go.einride.tech/mage-tools/mgpath"

	// mage:import
	"go.einride.tech/mage-tools/targets/mgyamlfmt"

	// mage:import
	"go.einride.tech/mage-tools/targets/mgconvco"

	// mage:import
	"go.einride.tech/mage-tools/targets/mggo"

	// mage:import
	"go.einride.tech/mage-tools/targets/mggolangcilint"

	// mage:import
	"go.einride.tech/mage-tools/targets/mgmarkdownfmt"

	// mage:import
	"go.einride.tech/mage-tools/targets/mggitverifynodiff"
)

func init() {
	mgmake.GenerateMakefiles(
		mgmake.Makefile{
			Path:          mgpath.FromGitRoot("Makefile"),
			DefaultTarget: All,
		},
	)
}

func All() {
	mg.Deps(
		mg.F(mgconvco.ConvcoCheck, "origin/master..HEAD"),
		mggolangcilint.GolangciLint,
		mgmarkdownfmt.FormatMarkdown,
		mgyamlfmt.FormatYaml,
		SpannerGenerate,
	)
	mg.SerialDeps(
		mggo.GoTest,
		mggo.GoModTidy,
		mggitverifynodiff.GitVerifyNoDiff,
	)
}

func SpannerGenerate() error {
	return sh.RunV("go", "run", ".", "generate")
}
