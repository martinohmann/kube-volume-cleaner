package config

import "github.com/spf13/cobra"

// Options holds the options that can be configured via cli flags.
type Options struct {
	Namespace     string
	LabelSelector string
	NoDelete      bool
}

// AddFlags adds cli flags for configurable options to the command.
func (o *Options) AddFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&o.NoDelete, "no-delete", o.NoDelete, "If set, delete actions will only be printed but not executed. This is useful for debugging.")
	cmd.Flags().StringVar(&o.Namespace, "namespace", o.Namespace, "Namespace to watch. If empty, all namespaces are watched.")
	cmd.Flags().StringVar(&o.LabelSelector, "label-selector", o.LabelSelector, "If set, only pvcs for statefulsets matching the label selector will be managed.")
}
