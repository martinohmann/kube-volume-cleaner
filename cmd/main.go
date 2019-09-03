package cmd

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/martinohmann/kube-volume-cleaner/pkg/config"
	"github.com/martinohmann/kube-volume-cleaner/pkg/controller"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
)

func init() {
	klog.InitFlags(flag.CommandLine)
	flag.Set("logtostderr", "true")

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
}

// NewRootCommand creates a new *cobra.Command that is used as the root command
// for kube-volume-cleaner.
func NewRootCommand() *cobra.Command {
	o := config.NewDefaultOptions()

	cmd := &cobra.Command{
		Use:          "kube-volume-cleaner",
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			err := o.Validate()
			if err != nil {
				return err
			}

			return Run(o)
		},
	}

	o.AddFlags(cmd)

	return cmd
}

// Execute creates and executes the root command. This is the main entrypoint
// for the application.
func Execute() {
	rootCmd := NewRootCommand()

	rootCmd.PersistentFlags().AddGoFlagSet(flag.CommandLine)

	if err := rootCmd.Execute(); err != nil {
		klog.Fatal(err)
	}
}

// Run sets up that controller and initiates the controller loop.
func Run(options *config.Options) error {
	client, err := newClient()
	if err != nil {
		return errors.Wrapf(err, "initializing kubernetes client failed")
	}

	klog.Infof("running with options: %+v", options)

	controller, err := controller.New(client, options)
	if err != nil {
		return errors.Wrapf(err, "failed to initialize controller")
	}

	ctx, cancel := context.WithCancel(context.Background())

	go handleSignals(cancel)

	return controller.Run(ctx.Done())
}

func handleSignals(cancelFunc func()) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
	<-signals
	klog.Info("received signal, terminating...")
	cancelFunc()
}

// newClient returns a new Kubernetes client with the default config.
func newClient() (kubernetes.Interface, error) {
	var kubeconfig string
	if _, err := os.Stat(clientcmd.RecommendedHomeFile); err == nil {
		kubeconfig = clientcmd.RecommendedHomeFile
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(config)
}
