package rancher

import (
	"fmt"
	"os"

	"github.com/golang/glog"

	"github.com/rancher/norman/clientbase"
	"github.com/rancher/norman/types"
	v3 "github.com/rancher/types/client/management/v3"

	"k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

type Config struct {
	URL         string
	Token       string
	ClusterName string
	ClusterID   string
}

func (c Config) Validate() error {
	if c.URL == "" {
		return fmt.Errorf("missing Rancher server URL")
	}

	if c.Token == "" {
		return fmt.Errorf("missing Rancher access token")
	}

	if c.ClusterName == "" && c.ClusterID == "" {
		return fmt.Errorf("missing Rancher cluster ID")
	}

	return nil
}

type RancherManager struct {
	nodes     v3.NodeOperations
	nodePools v3.NodePoolOperations
	clusterId string
}

func BuildRancherManager() (*RancherManager, error) {
	cfg := Config{
		URL:			os.Getenv("RANCHER_URL"),
		Token:			os.Getenv("RANCHER_TOKEN"),
		ClusterName:	os.Getenv("RANCHER_CLUSTER_NAME"),
		ClusterID:		os.Getenv("RANCHER_CLUSTER_ID"),
	}

	err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	clientOpts := &clientbase.ClientOpts{
		URL:        cfg.URL,
		TokenKey:   cfg.Token,
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
		nodes: 		rancherClient.Node,
		nodePools:	rancherClient.NodePool,
		clusterId:	clusterId,
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
	} else {
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
}

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
					id:		nodePool.ID,
				})
			}
		}
	}

	return nodeGroups, nil
}

func (m *RancherManager) GetCachedNodePool(id string) (*v3.NodePool, error) {
	np, err := m.nodePools.ByID(id)
	if err != nil {
		return nil, err
	}

	return np, nil
}

func (m *RancherManager) IncreasePoolSize(id string, delta int) error {
	// we need a fresh node pool object instead of a cached one
	np, err := m.nodePools.ByID(id)
	if err != nil {
		return err
	}

	targetQuantity := np.Quantity + int64(delta)
	glog.Infof("Increasing %s capacity from %d to %d", np.HostnamePrefix, np.Quantity, targetQuantity)

	update := make(map[string]interface{})
	update[v3.NodePoolFieldQuantity] = targetQuantity

	_, err = m.nodePools.Update(np, update)
	if err != nil {
		return err
	}

	return nil
}

func (m *RancherManager) DeleteNodes(nodePoolId string, nodes []*v1.Node) error {
	for _, node := range nodes {
		if err := m.DeleteNode(nodePoolId, node); err != nil {
			return err
		}
	}

	return nil
}

func (m *RancherManager) DeleteNode(nodePoolId string, node *v1.Node) error {
	rancherNode, err := m.GetNodeForKubernetesNode(node.Name)
	if err != nil {
		return err
	}

	if rancherNode == nil || rancherNode.NodePoolID != nodePoolId {
		return fmt.Errorf("node %s not found", node.Name)
	}

	glog.Infof("Marking node %s (%s) for deletion", rancherNode.ID, rancherNode.NodeName)
	err = m.nodes.ActionPleaseKillMe(rancherNode)
	if err != nil {
		return err
	}

	return nil
}

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

func (m *RancherManager) GetCachedNodeForKubernetesNode(name string) (*v3.Node, error) {
	return m.GetNodeForKubernetesNode(name)
}

func (m *RancherManager) GetNodeForKubernetesNode(nodeName string) (*v3.Node, error) {
	listOpts := &types.ListOpts{
		Filters: map[string]interface{}{
			v3.NodeFieldClusterID: m.clusterId,
			v3.NodeFieldNodeName: nodeName,
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

func (m *RancherManager) Cleanup() error {
	return nil
}

func (m *RancherManager) Refresh() error {
	return nil
}

func hasAutoScalingEnabled(nodePool *v3.NodePool) bool {
	return nodePool.Annotations[autoScalingEnabledAnnotation] == "true"
}
