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
	"os"
	"time"

	"github.com/rancher/norman/clientbase"
	"github.com/rancher/norman/types"
	v3 "github.com/rancher/rancher/pkg/client/generated/management/v3"

	"k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	klog "k8s.io/klog/v2"
)

const (
	nodeDeletionVerifierRetryLimit = 10
	nodeDeletionVerifierRetryDelay = 5 * time.Second
)

// Config contains configuration attributes for Rancher communication
type Config struct {
	URL         string
	Token       string
	ClusterName string
	ClusterID   string
}

// Validate the configuration attributes
func (c Config) Validate() error {
	if c.URL == "" {
		return fmt.Errorf("missing Rancher server URL")
	}

	if c.Token == "" {
		return fmt.Errorf("missing Rancher access token")
	}

	if c.ClusterName == "" && c.ClusterID == "" {
		return fmt.Errorf("missing Rancher cluster ID or name")
	}

	return nil
}

// RancherManager handles Rancher communication and data caching
type RancherManager struct {
	nodes     v3.NodeOperations
	nodePools v3.NodePoolOperations
	clusterId string
}

// BuildRancherManager builds a new Rancher Manager object to work with Rancher
func BuildRancherManager() (*RancherManager, error) {
	cfg := Config{
		URL:         os.Getenv("RANCHER_URL"),
		Token:       os.Getenv("RANCHER_TOKEN"),
		ClusterName: os.Getenv("RANCHER_CLUSTER_NAME"),
		ClusterID:   os.Getenv("RANCHER_CLUSTER_ID"),
	}

	err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	clientOpts := &clientbase.ClientOpts{
		URL:      cfg.URL,
		TokenKey: cfg.Token,
	}

	rancherClient, err := v3.NewClient(clientOpts)
	if err != nil {
		return nil, err
	}

	clusterId, err := findCluster(rancherClient, cfg)
	if err != nil {
		return nil, err
	}

	m := &RancherManager{
		nodes:     rancherClient.Node,
		nodePools: rancherClient.NodePool,
		clusterId: clusterId,
	}

	return m, nil
}

func findCluster(client *v3.Client, config Config) (string, error) {
	if config.ClusterID != "" {
		_, err := client.Cluster.ByID(config.ClusterID)
		if err != nil {
			return "", fmt.Errorf("failed to find cluster %s: %s", config.ClusterID, err)
		}

		return config.ClusterID, nil
	}

	listOpts := &types.ListOpts{
		Filters: map[string]interface{}{
			v3.ClusterFieldName: config.ClusterName,
		},
	}

	clusters, err := client.Cluster.List(listOpts)
	if err != nil {
		return "", fmt.Errorf("failed to find cluster %s: %s", config.ClusterName, err)
	}

	if len(clusters.Data) != 1 {
		return "", fmt.Errorf("cluster %s not found", config.ClusterName)
	}

	return clusters.Data[0].ID, nil
}

// GetCachedNodePools returns a (currently uncached) list of known NodeGroup objects
func (m *RancherManager) GetCachedNodePools() ([]cloudprovider.NodeGroup, error) {
	listOpts := &types.ListOpts{
		Filters: map[string]interface{}{
			v3.NodePoolFieldClusterID: m.clusterId,
		},
	}

	nodeGroups := make([]cloudprovider.NodeGroup, 0)

	for nodePools, err := m.nodePools.List(listOpts); nodePools != nil || err != nil; nodePools, err = nodePools.Next() {
		if err != nil {
			return nil, err
		}

		for _, nodePool := range nodePools.Data {
			if hasAutoScalingEnabled(&nodePool) {
				nodeGroups = append(nodeGroups, &rancherNodeGroup{
					manager: m,
					id:      nodePool.ID,
				})
			}
		}
	}

	return nodeGroups, nil
}

// GetCachedNodePool returns a (currently uncached) NodePool object given its id
func (m *RancherManager) GetCachedNodePool(id string) (*v3.NodePool, error) {
	np, err := m.nodePools.ByID(id)
	if err != nil {
		return nil, err
	}

	return np, nil
}

// IncreasePoolSize increases the size of the pool identified by id by delta
func (m *RancherManager) IncreasePoolSize(id string, delta int) error {
	// we need a fresh node pool object instead of a cached one
	np, err := m.nodePools.ByID(id)
	if err != nil {
		return err
	}

	targetQuantity := np.Quantity + int64(delta)
	klog.Infof("Increasing %s capacity from %d to %d", np.HostnamePrefix, np.Quantity, targetQuantity)

	update := make(map[string]interface{})
	update[v3.NodePoolFieldQuantity] = targetQuantity

	_, err = m.nodePools.Update(np, update)
	if err != nil {
		return err
	}

	return nil
}

// DeleteNodes deletes multiple nodes from the cluster
func (m *RancherManager) DeleteNodes(nodePoolId string, nodes []*v1.Node) error {
	for _, node := range nodes {
		if err := m.DeleteNode(nodePoolId, node); err != nil {
			return err
		}
	}

	return nil
}

// DeleteNode deletes a single node from the cluster
func (m *RancherManager) DeleteNode(nodePoolId string, node *v1.Node) error {
	rancherNode, err := m.GetNodeForKubernetesNode(node.Name)
	if err != nil {
		return err
	}

	if rancherNode == nil || rancherNode.NodePoolID != nodePoolId {
		return fmt.Errorf("node %s not found", node.Name)
	}

	klog.Infof("Marking node %s (%s) for deletion", rancherNode.ID, rancherNode.NodeName)
	err = m.nodes.ActionPleaseKillMe(rancherNode)
	if err != nil {
		return err
	}

	// cluster autoscaler specifies that we should wait until the node disappears from the underlying infrastructure
	// before proceeding so we'll park here for a while
	for retry := 0; retry < nodeDeletionVerifierRetryLimit; retry++ {
		time.Sleep(nodeDeletionVerifierRetryDelay)

		_, err := m.nodes.ByID(rancherNode.ID)
		if err == nil {
			continue
		}

		if clientbase.IsNotFound(err) {
			// yay :)
			return nil
		}

		klog.Warningf("unexpected error while checking node deletion (attempt %d/%d): %s",
			retry, nodeDeletionVerifierRetryLimit, err)

	}

	return fmt.Errorf("unable to confirm node %s (%s) deletion for %s",
		rancherNode.ID, rancherNode.Name, nodeDeletionVerifierRetryLimit*nodeDeletionVerifierRetryDelay)
}

// GetCachedNodePoolNodes returns a (currently uncached) list of Nodes belonging to a NodePool
func (m *RancherManager) GetCachedNodePoolNodes(id string) ([]*v3.Node, error) {
	listOpts := &types.ListOpts{
		Filters: map[string]interface{}{
			v3.NodeFieldNodePoolID: id,
		},
	}

	nodePoolNodes := make([]*v3.Node, 0)

	for nodes, err := m.nodes.List(listOpts); nodes != nil || err != nil; nodes, err = nodes.Next() {
		if err != nil {
			return nil, err
		}

		for _, node := range nodes.Data {
			nodePoolNodes = append(nodePoolNodes, &node)
		}
	}

	return nodePoolNodes, nil
}

// GetCachedNodeForKubernetesNode returns a (currently uncached) Node object from Rancher given its Kubernetes' name
func (m *RancherManager) GetCachedNodeForKubernetesNode(name string) (*v3.Node, error) {
	return m.GetNodeForKubernetesNode(name)
}

// GetNodeForKubernetesNode returns a Node object from Rancher given its Kubernetes' name
func (m *RancherManager) GetNodeForKubernetesNode(nodeName string) (*v3.Node, error) {
	listOpts := &types.ListOpts{
		Filters: map[string]interface{}{
			v3.NodeFieldClusterID: m.clusterId,
			v3.NodeFieldNodeName:  nodeName,
		},
	}

	nodes, err := m.nodes.List(listOpts)
	if err != nil {
		return nil, err
	}

	if len(nodes.Data) != 1 {
		return nil, fmt.Errorf("node %s not found", nodeName)
	}

	return &nodes.Data[0], nil
}

// Cleanup disposes of resources (such as goroutines, once we have caching)
func (m *RancherManager) Cleanup() error {
	return nil
}

// Refresh invalidates the caches (once we have them)
func (m *RancherManager) Refresh() error {
	return nil
}

func hasAutoScalingEnabled(nodePool *v3.NodePool) bool {
	return nodePool.Annotations[autoScalingEnabledAnnotation] == "true"
}
