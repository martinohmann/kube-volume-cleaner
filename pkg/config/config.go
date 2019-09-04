package config

import (
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

const (
	// DefaultResyncInterval is the default interval for the controller to
	// resync PersistentVolumeClaims.
	DefaultResyncInterval = 30 * time.Second

	// DefaultControllerID is the default ID of the controller used to annotate
	// PersistentVolumeClaims with in order to allow multiple controllers to
	// coexist without interfering.
	DefaultControllerID = "kube-volume-cleaner"
)

// Options holds the options that can be configured via cli flags.
type Options struct {
	ControllerID   string
	Namespace      string
	LabelSelector  string
	NoDelete       bool
	DeleteAfter    time.Duration
	ResyncInterval time.Duration
}

// NewDefaultOptions creates a new *Options value with defaults set.
func NewDefaultOptions() *Options {
	return &Options{
		ControllerID:   DefaultControllerID,
		ResyncInterval: DefaultResyncInterval,
	}
}

// AddFlags adds cli flags for configurable options to the command.
func (o *Options) AddFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&o.NoDelete, "no-delete", o.NoDelete, "If set, delete actions will only be printed but not executed. This is useful for debugging.")
	cmd.Flags().StringVar(&o.ControllerID, "controller-id", o.ControllerID, "ID of the controller. Must be unique across the cluster and allows multiple instances of kube-volume-cleaner to coexist without interfering with each other.")
	cmd.Flags().StringVar(&o.Namespace, "namespace", o.Namespace, "Namespace to watch. If empty, all namespaces are watched.")
	cmd.Flags().StringVar(&o.LabelSelector, "label-selector", o.LabelSelector, "If set, only pvcs for statefulsets matching the label selector will be managed.")
	cmd.Flags().DurationVar(&o.DeleteAfter, "delete-after", o.DeleteAfter, "Duration after which to delete pvcs belonging to a deleted statefulset.")
	cmd.Flags().DurationVar(&o.ResyncInterval, "resync-interval", o.ResyncInterval, "Duration after the status of all pvcs should be resynced to perform cleanup.")
}

// Validate validates options.
func (o *Options) Validate() error {
	if o.ControllerID == "" {
		return errors.Errorf("--controller-id must not be empty")
	}

	if o.DeleteAfter < 0 {
		return errors.Errorf("--delete-after has to be greater than or equal to 0s")
	}

	if o.ResyncInterval < time.Second {
		return errors.Errorf("--resync-interval has to be greater than or equal to 1s")
	}

	return nil
}
