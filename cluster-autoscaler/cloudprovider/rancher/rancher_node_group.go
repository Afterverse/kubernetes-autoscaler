/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rancher

import (
	"fmt"
	"strconv"

	v3 "github.com/rancher/rancher/pkg/client/generated/management/v3"

	"k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/klog"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

const (
	defaultMaxSize = 400
	defaultMinSize = 1

	autoScalingEnabledAnnotation = "nodepools.pkds.it/enable-autoscaling"
	maxSizeAnnotation            = "nodepools.pkds.it/max-size"
	minSizeAnnotation            = "nodepools.pkds.it/min-size"
)

// NodeGroup contains configuration info and functions to control a set
// of nodes that have the same capacity and set of labels.
type rancherNodeGroup struct {
	manager *RancherManager
	id      string
}

func (ng *rancherNodeGroup) nodePool() *v3.NodePool {
	nodePool, err := ng.manager.GetCachedNodePool(ng.id)
	if err != nil {
		klog.Fatalf("failed to get node pool %s: %s", ng.id, err)
	}

	return nodePool
}

// MaxSize returns maximum size of the node group.
func (ng *rancherNodeGroup) MaxSize() int {
	nodePool := ng.nodePool()

	if maxSizeValue, exists := nodePool.Annotations[maxSizeAnnotation]; exists {
		maxSize, err := strconv.Atoi(maxSizeValue)
		if err != nil {
			klog.Errorf("failed to parse max size from node pool %s: %s", nodePool.HostnamePrefix, err)
			return defaultMaxSize
		}

		return maxSize
	}

	return defaultMaxSize
}

// MinSize returns minimum size of the node group.
func (ng *rancherNodeGroup) MinSize() int {
	nodePool := ng.nodePool()

	if minSizeValue, exists := nodePool.Annotations[minSizeAnnotation]; exists {
		minSize, err := strconv.Atoi(minSizeValue)
		if err != nil {
			klog.Errorf("failed to parse min size from node pool %s: %s", nodePool.HostnamePrefix, err)
			return defaultMinSize
		}

		return minSize
	}

	return defaultMinSize
}

// TargetSize returns the current target size of the node group. It is possible that the
// number of nodes in Kubernetes is different at the moment but should be equal
// to Size() once everything stabilizes (new nodes finish startup and registration or
// removed nodes are deleted completely). Implementation required.
func (ng *rancherNodeGroup) TargetSize() (int, error) {
	return int(ng.nodePool().Quantity), nil
}

// IncreaseSize increases the size of the node group. To delete a node you need
// to explicitly name it and use DeleteNode. This function should wait until
// node group size is updated. Implementation required.
func (ng *rancherNodeGroup) IncreaseSize(delta int) error {
	return ng.manager.IncreasePoolSize(ng.id, delta)
}

// DeleteNodes deletes nodes from this node group. Error is returned either on
// failure or if the given node doesn't belong to this node group. This function
// should wait until node group size is updated. Implementation required.
func (ng *rancherNodeGroup) DeleteNodes(nodes []*v1.Node) error {
	return ng.manager.DeleteNodes(ng.id, nodes)
}

// DecreaseTargetSize decreases the target size of the node group. This function
// doesn't permit to delete any existing node and can be used only to reduce the
// request for new nodes that have not been yet fulfilled. Delta should be negative.
// It is assumed that cloud provider will not delete the existing nodes when there
// is an option to just decrease the target. Implementation required.
func (ng *rancherNodeGroup) DecreaseTargetSize(delta int) error {
	return fmt.Errorf("sry but I won't decrease the target size ¯\\_(ツ)_/¯")
}

// Id returns an unique identifier of the node group.
func (ng *rancherNodeGroup) Id() string {
	return ng.nodePool().ID
}

// Debug returns a string containing all information regarding this node group.
func (ng *rancherNodeGroup) Debug() string {
	nodePool := ng.nodePool()
	return fmt.Sprintf("%s - %s (%d:%d - actual: %d)", ng.Id(),
		nodePool.HostnamePrefix, ng.MinSize(), ng.MaxSize(), nodePool.Quantity)
}

// Nodes returns a list of all nodes that belong to this node group.
// It is required that Instance objects returned by this method have Id field set.
// Other fields are optional.
func (ng *rancherNodeGroup) Nodes() ([]cloudprovider.Instance, error) {
	nodes, err := ng.manager.GetCachedNodePoolNodes(ng.id)
	if err != nil {
		return nil, err
	}

	return toInstances(nodes), nil
}

// TemplateNodeInfo returns a schedulernodeinfo.NodeInfo structure of an empty
// (as if just started) node. This will be used in scale-up simulations to
// predict what would a new node look like if a node group was expanded. The returned
// NodeInfo is expected to have a fully populated Node object, with all of the labels,
// capacity and allocatable information as well as all pods that are started on
// the node by default, using manifest (most likely only kube-proxy). Implementation optional.
func (ng *rancherNodeGroup) TemplateNodeInfo() (*schedulernodeinfo.NodeInfo, error) {
	// TODO we might want to implement this in the future
	return nil, cloudprovider.ErrNotImplemented
}

// Exist checks if the node group really exists on the cloud provider side. Allows to tell the
// theoretical node group from the real one. Implementation required.
func (ng *rancherNodeGroup) Exist() bool {
	return true
}

// Create creates the node group on the cloud provider side. Implementation optional.
func (ng *rancherNodeGroup) Create() (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

// Delete deletes the node group on the cloud provider side.
// This will be executed only for autoprovisioned node groups, once their size drops to 0.
// Implementation optional.
func (ng *rancherNodeGroup) Delete() error {
	return cloudprovider.ErrNotImplemented
}

// Autoprovisioned returns true if the node group is autoprovisioned. An autoprovisioned group
// was created by CA and can be deleted when scaled to 0.
func (ng *rancherNodeGroup) Autoprovisioned() bool {
	return false
}

// toInstances converts a slice of *v3.Node to cloudprovider.Instance
func toInstances(nodes []*v3.Node) []cloudprovider.Instance {
	instances := make([]cloudprovider.Instance, len(nodes))
	for i, node := range nodes {
		instances[i] = toInstance(node)
	}
	return instances
}

// toInstance converts the given *v3.Node to a cloudprovider.Instance
func toInstance(node *v3.Node) cloudprovider.Instance {
	return cloudprovider.Instance{
		Id:     node.ID,
		Status: nil, // TODO we might want to implement this in the future
	}
}
