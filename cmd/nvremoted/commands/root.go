// Copyright © 2023 Niko Carpenter <niko@nikocarpenter.com>
//
// This source code is governed by the MIT license, which can be found in the LICENSE file.

package commands

import (
	"context"
	"fmt"
	"os"
	"path"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgDir string

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "nvremoted",
	Short: "NVDA Remote server",
	Long: `NVRemoted is a server for NVDA Remote.

This application relays messages for NVDA Remote clients,
and prints usage stats for other NVRemoted servers.`,
	SilenceErrors:     true,
	SilenceUsage:      true,
	DisableAutoGenTag: true,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return initConfig(cmd.Context())
	},
}

// Execute runs Cobra with process cancellation and a bounded stop budget.
// It is called once by main; errors are reported at that process exit boundary.
func Execute(ctx context.Context) error {
	return supervise(ctx, shutdownTimeout, func(ctx context.Context) error {
		return RootCmd.ExecuteContext(ctx)
	})
}

func init() {
	RootCmd.PersistentFlags().StringVar(&cfgDir, "config", "", "config directory (default is $HOME/.config/nvremoted)")
}

// initConfig reads in config file and ENV variables if set.
func initConfig(ctx context.Context) error {
	if err := stopError(ctx); err != nil {
		return err
	}
	if cfgDir == "" {
		home, err := homedir.Dir()
		if err != nil {
			return fmt.Errorf("find home directory: %w", err)
		}
		cfgDir = path.Join(home, ".config", "nvremoted")
	}
	if err := stopError(ctx); err != nil {
		return err
	}
	viper.AddConfigPath(cfgDir)
	viper.SetConfigName("nvremoted")
	if err := os.Setenv("CONFDIR", cfgDir); err != nil {
		return fmt.Errorf("set config directory: %w", err)
	}
	return readConfig(ctx, viper.ReadInConfig)
}

func readConfig(ctx context.Context, read func() error) error {
	if err := stopError(ctx); err != nil {
		return err
	}
	if err := read(); err != nil {
		return fmt.Errorf("load config file: %w", err)
	}
	return stopError(ctx)
}
