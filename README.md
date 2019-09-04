kube-volume-cleaner
===================

[![Build Status](https://travis-ci.org/martinohmann/kube-volume-cleaner.svg?branch=master)](https://travis-ci.org/martinohmann/kube-volume-cleaner)
[![codecov](https://codecov.io/gh/martinohmann/kube-volume-cleaner/branch/master/graph/badge.svg)](https://codecov.io/gh/martinohmann/kube-volume-cleaner)
[![Go Report Card](https://goreportcard.com/badge/github.com/martinohmann/kube-volume-cleaner?style=flat)](https://goreportcard.com/report/github.com/martinohmann/kube-volume-cleaner)
[![GoDoc](https://godoc.org/github.com/martinohmann/kube-volume-cleaner?status.svg)](https://godoc.org/github.com/martinohmann/kube-volume-cleaner)

A Kubernetes controller that can manage and delete `PersistentVolumeClaims` created by `StatefulSets`.

Contents
--------

- [Before deploying `kube-volume-cleaner`](#before-deploying-kube-volume-cleaner)
- [Why?](#why)
- [How does it work?](#how-does-it-work)
- [Installation](#installation)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Limitations](#limitations)
- [License](#license)

Before deploying `kube-volume-cleaner`
--------------------------------------

**This controller deletes data!** Do not use it if you cannot affort losing
data contained in the volumes backed by PersistentVolumeClaim resources or if
you do not have proper backup and restore infrastructure in place. Always
backup your data! **I will take no responsibility for accidental data loss. You
have been warned!**

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

Installation
------------

```sh
go get -u github.com/martinohmann/kube-volume-cleaner
```

WIP

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

WIP

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
