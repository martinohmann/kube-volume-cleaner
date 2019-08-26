module github.com/martinohmann/kube-volume-cleaner

go 1.12

require (
	github.com/pkg/errors v0.8.1
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.3
	github.com/stretchr/testify v1.3.0
	k8s.io/api v0.0.0-20190808180749-077ce48e77da
	k8s.io/apimachinery v0.0.0-20190808180622-ac5d3b819fc6
	k8s.io/client-go v0.0.0-20190808180953-396a06da3bd7
	k8s.io/klog v0.3.3
)
