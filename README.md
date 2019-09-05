kube-volume-cleaner
===================

[![Build Status](https://travis-ci.org/martinohmann/kube-volume-cleaner.svg?branch=master)](https://travis-ci.org/martinohmann/kube-volume-cleaner)
[![codecov](https://codecov.io/gh/martinohmann/kube-volume-cleaner/branch/master/graph/badge.svg)](https://codecov.io/gh/martinohmann/kube-volume-cleaner)
[![Go Report Card](https://goreportcard.com/badge/github.com/martinohmann/kube-volume-cleaner?style=flat)](https://goreportcard.com/report/github.com/martinohmann/kube-volume-cleaner)
[![GoDoc](https://godoc.org/github.com/martinohmann/kube-volume-cleaner?status.svg)](https://godoc.org/github.com/martinohmann/kube-volume-cleaner)

A Kubernetes controller that can manage and delete `PersistentVolumeClaims` created by `StatefulSets`.

Contents
--------

- [Why?](#why)
- [How does it work?](#how-does-it-work)
  - [Unused PVCs after a StatefulSet was scaled down](#unused-pvcs-after-a-statefulset-was-scaled-down)
  - [Orphaned Pods](#orphaned-pods)
- [Before using kube-volume-cleaner](#before-using-kube-volume-cleaner)
- [Installation](#installation)
  - [Deploy to Kubernetes](#deploy-to-kubernetes)
  - [Running with Docker](#running-with-docker)
  - [Local installation](#local-installation)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Annotations](#annotations)
- [Limitations](#limitations)
- [License](#license)

Why?
----

Especially in cloud environments, storage can get very expensive. Thus it is
crucial to release resources when they are not needed anymore. In automated
testing or staging environments in shared clusters you can end up with lots of
volumes that are only needed for a short period of time. Sadly, currently there
is no way to configure StatefulSets to delete PersistentVolumeClaims they
provisioned after the StatefulSet disappeared, causing the underlying storage
resources not to be released. This controller tried to make this deletion
process configurable and automatic.

How does it work?
-----------------

The controller watches for the creation, update and deletion of Pods,
StatefulSets and PersistentVolumeClaims. Whenever a PersistentVolumeClaim is
created or updated, the controller annotates the PersistentVolumeClaim with the
name of the StatefulSet if the mounting Pod belongs to one. Based on these
annotations and StatefulSet deletion events it can make decisions about whether
a PersistentVolumeClaim should be deleted or not.

The controller can be configured to delete PersistentVolumeClaim right after
the StatefulSet is gone or PersistentVolumeClaims can be marked with a
timestamp in the future to delay the delete operation (e.g. delete after it is
unused for 7 days). It is also possible to run multiple controllers in the same
cluster which supervise different namespaces or groups of StatefulSets (via
label selector) and which may have different PVC retention policies (e.g.
delete immediately vs. delete after x hours/days/months etc.). See the
[configuration](#configuration) section for all available options.

### Unused PVCs after a StatefulSet was scaled down

If a StatefulSet is scaled down some PVCs become unused. These will not be cleaned by the controller immediately. Instead, all PVCs every created by the StatefulSet are only ever deleted when the StatefulSet and all of its Pods are deleted.

### Orphaned Pods

StatefulSets can produce orphaned pods e.g. because they were delete via
`kubectl delete statefulset foobar --cascade=false`). These pods still mount PVCs that were provisioned by the initial StatefulSet. The controller is smart enough to notice this and to not make any attempts to delete these PVCs once the pod is terminated or restarted.

You can easily bring these PVCs back under the control of kube-volume-cleaner by deploying a StatefulSet that will pick up the orphaned pods.

Before using kube-volume-cleaner
--------------------------------

**This controller deletes data!** Do not use it if you cannot affort losing
data contained in the volumes backed by PersistentVolumeClaim resources or if
you do not have proper backup and restore infrastructure in place. Always
backup your data! **I will take no responsibility for accidental data loss. You
have been warned!**

Installation
------------

### Deploy to Kubernetes

The Kubernetes manifests can be found in the [deploy/](deploy/) subfolder and
can be deployed via `kubectl`:

```sh
kubectl apply -f deploy/
```

This will install a ServiceAccount and a Deployment for the controller in the
`kube-system` namespace as well as the necessary ClusterRole and
ClusterRoleBinding. You may use customized versions of these manifests.

### Running with Docker

```
docker run -v $HOME/.kube/config:/.kube/config mohmann/kube-volume-cleaner:latest --help
```

Also be sure to check out the available image version at [Docker Hub](https://hub.docker.com/r/mohmann/kube-volume-cleaner).

### Local installation

Although not recommended you can install the controller locally and run it via:

```sh
go get -u github.com/martinohmann/kube-volume-cleaner
kube-volume-cleaner --help
```

The controller will use the currently set context in your `$HOME/.kube/config`
file.

Prerequisites
-------------

The controller only deletes PersistentVolumeClaims but not the underlying
PersistentVolume. For volumes to get automatically removed after the
PersistentVolumeClaim is removed, the StatefulSet needs to provision
PersistentVolumeClaims with a StorageClass with `reclaimPolicy: Delete` and a
`provisioner` that supports deletion of the underlying storage volume. E.g.:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp2-auto-delete
provisioner: kubernetes.io/aws-ebs
parameters:
  type: gp2
reclaimPolicy: Delete
allowVolumeExpansion: true
mountOptions:
- debug
volumeBindingMode: Immediate
```

Make use of the StorageClass in the StatefulSet's volume claim template spec:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: my-statefulset
spec:
  replicas: 1
  template:
    ...
  volumeClaimTemplates:
  - metadata:
      name: my-claim
    spec:
      accessModes:
      - ReadWriteOnce
      storageClassName: gp2-auto-delete
      resources:
        requests:
          storage: 10Mi
```

Configuration
-------------

This is a list of all important commandline flags that are exposed by the controller:

| Flag            | Default Value                               | Description                                                                                                                                               |
| ------------------- | ------------- | ----- |
| `--controller-id`   | `kube-volume-cleaner`                           | ID of the controller. Must be unique across the cluster and allows multiple instances of kube-volume-cleaner to coexist without interfering with each other.
| `--no-delete`       | `false`                                         | If set, delete actions will only be printed but not executed. This is useful for debugging.                                                                  |
| `--namespace`       | all namespaces                                  | Namespace to watch. If empty, all namespaces are watched.                                                                                                     |
| `--label-selector`  | `kube-volume-cleaner.io/on-delete=cleanup-pvcs` | Only pvcs for statefulsets matching the label selector will be managed. Can be set to an empty string if you want to have the controller manage all pvcs.    |
| `--delete-after`    | `24h`                                           | Duration after which to delete pvcs belonging to a deleted statefulset.                                                                                       |
| `--resync-interval` | `30s`                                           | Duration after the status of all pvcs should be resynced to perform cleanup.                                                                                  |

The controller also exposes all [`klog`](https://github.com/kubernetes/klog)
flags to control the logging behaviour. Run the controller with the `--help`
flag to see all available options.

Annotations
-----------

The controller uses annotations on PersistentVolumeClaims to keep track of the
relations between the PVC and a StatefulSet (and the controller itself) and,
depending on the configuration, about its scheduled deletion time. These annotations should be left untouched (unless you really know what you are doing).

* `statefulset.kube-volume-cleaner.io/managed-by` is added to
  PersistentVolumeClaims that the controller identifies as managed by a
  StatefulSet. The value is always the name of the managing StatefulSet in the
  same namespace as the PersistentVolumeClaim. The annotation is used to
  identify PersistentVolumeClaims after the deletion of a StatefulSet.
* `pvc.kube-volume-cleaner.io/delete-after` is added to
  PersistentVolumeClaims that should be deleted only after the specified moment
  in time. The annotation is set once the managing StatefulSet and the Pod
  mounting the PersistentVolumeClaim are deleted and the controller is
  configured to run with a delete-after value > 0s.
* `kube-volume-cleaner.io/controller-id` signals the ownership of a
  PersistentVolumeClaim to other kube-volume-cleaner instances. Controllers
  must not touch PersistentVolumeClaims where the value of this annotation does
  not match their own ID.

Limitations
-----------

The controller will react to events that happenend while it was not running
(e.g. when it is rescheduled onto a different node). However, there is an
edge-case that cannot be handled: if a StatefulSet gets created and immediately
deleted while the controller is not running, it cannot identify the
PersistentVolumeClaims that need to be cleaned, but the probability for this to
happen should be relatively low.

License
-------

The source code of kube-volume-cleaner is released under the MIT
License. See the bundled LICENSE file for details.
