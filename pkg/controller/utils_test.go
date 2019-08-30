package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func TestPodHasVolumeClaim(t *testing.T) {
	tests := []struct {
		name      string
		pod       *corev1.Pod
		claimName string
		expected  bool
	}{
		{
			name: "pod with matching claim",
			pod: newPodWithVolumes("foo", "default", []corev1.Volume{
				newVolumeWithClaim("some-vol", "bar"),
			}),
			claimName: "bar",
			expected:  true,
		},
		{
			name: "pod with other volumes and matching claim",
			pod: newPodWithVolumes("foo", "default", []corev1.Volume{
				newVolumeWithHostPath("host-vol", "/var/run"),
				newVolumeWithClaim("some-vol", "bar"),
				newVolumeWithClaim("some-other-vol", "baz"),
			}),
			claimName: "bar",
			expected:  true,
		},
		{
			name: "pod without matching claim",
			pod: newPodWithVolumes("foo", "default", []corev1.Volume{
				newVolumeWithClaim("some-vol", "foo"),
			}),
			claimName: "bar",
			expected:  false,
		},
		{
			name:      "pod without volumes",
			pod:       newPod("foo", "default"),
			claimName: "bar",
			expected:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, podHasVolumeClaim(test.pod, test.claimName))
		})
	}
}
