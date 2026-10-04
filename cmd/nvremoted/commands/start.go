// Copyright © 2023 Niko Carpenter <niko@nikocarpenter.com>
//
// This source code is governed by the MIT license, which can be found in the LICENSE file.

package commands

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"time"

	"github.com/n0ot/nvremoted/pkg/server"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var disableTLS bool

// startCmd represents the start command
var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Starts the NVRemoted server",
	RunE:  runServer,
}

func init() {
	RootCmd.AddCommand(startCmd)

	startCmd.Flags().StringP("bind", "b", "127.0.0.1:6837", "Bind the server to host:port. Leave host empty to bind to all interfaces.")
	viper.BindPFlag("server.bind", startCmd.Flags().Lookup("bind"))
	startCmd.Flags().IntP("time-between-pings", "t", 30, "How often pings should be sent in seconds (0 disables)")
	viper.BindPFlag("server.timeBetweenPings", startCmd.Flags().Lookup("time-between-pings"))
	startCmd.Flags().IntP("pings-until-timeout", "p", 0, "Deprecated: accepted for compatibility, ignored (clients do not acknowledge pings)")
	viper.BindPFlag("server.pingsUntilTimeout", startCmd.Flags().Lookup("pings-until-timeout"))
	startCmd.Flags().BoolVarP(&disableTLS, "disable-tls", "d", false, "Overrides config option to enable TLS")

	viper.SetDefault("server.statsPassword", "")
	viper.SetDefault("tls.useTls", true)
}

func runServer(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	var log *logrus.Logger
	var tlsConfig *tls.Config
	bindAddr := viper.GetString("server.bind")
	useTLS := viper.GetBool("tls.useTls") && !disableTLS
	err := startServer(ctx, func(ctx context.Context) (runningServer, error) {
		log = logrus.New()
		log.Out = os.Stderr
		log.Formatter = new(logrus.TextFormatter)
		log.Level = logrus.DebugLevel
		log.Info("Starting NVRemoted")
		if err := stopError(ctx); err != nil {
			return nil, err
		}
		admission, err := readAdmissionConfig(viper.GetViper())
		if err != nil {
			return nil, fmt.Errorf("admission configuration: %w", err)
		}

		var motd string
		motdFile := os.ExpandEnv(viper.GetString("nvremoted.motdFile"))
		if motdBuf, err := ioutil.ReadFile(motdFile); err == nil {
			motd = string(motdBuf)
		}
		if err := stopError(ctx); err != nil {
			return nil, err
		}
		srv := &server.Server{
			Admission:         admission,
			TimeBetweenPings:  viper.GetDuration("server.timeBetweenPings") * time.Second,
			PingsUntilTimeout: viper.GetInt("server.pingsUntilTimeout"),
			MOTD:              strings.TrimSpace(motd),
			StatsPassword:     viper.GetString("server.statsPassword"),
			Log:               log,
		}
		if err := stopError(ctx); err != nil {
			return nil, err
		}
		if useTLS {
			certFile := os.ExpandEnv(viper.GetString("tls.certFile"))
			keyFile := os.ExpandEnv(viper.GetString("tls.keyFile"))
			var err error
			tlsConfig, err = loadTLSConfig(ctx, certFile, keyFile, tls.LoadX509KeyPair)
			if err != nil {
				return nil, err
			}
		}
		return srv, nil
	}, func(ctx context.Context) (net.Listener, error) {
		var lc net.ListenConfig
		listener, err := lc.Listen(ctx, "tcp", bindAddr)
		if err != nil {
			return nil, fmt.Errorf("listen: %w", err)
		}
		if useTLS {
			listener = tls.NewListener(listener, tlsConfig)
		}
		return listener, nil
	}, shutdownTimeout)
	if ctx.Err() != nil && (err == nil || err == errStopRequested) && log != nil {
		log.Info("NVRemoted stopped")
	}
	return err
}

// The loader cannot interrupt an in-progress filesystem read. Check both sides
// and preserve a required-read error even if cancellation happened meanwhile.
func loadTLSConfig(ctx context.Context, certFile, keyFile string, load func(string, string) (tls.Certificate, error)) (*tls.Config, error) {
	if err := stopError(ctx); err != nil {
		return nil, err
	}
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("No TLSConfig set in server, and no certFile/keyFile given")
	}
	cert, err := load(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load X.509 key pair: %w", err)
	}
	if err := stopError(ctx); err != nil {
		return nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
}
