package redis

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	redis "github.com/redis/go-redis/v9"
)

type Client struct{}

func NewClient() Client {
	return Client{}
}

const (
	sentinelsCountPattern   = "sentinels=([0-9]+)"
	slavesCountPattern      = "slaves=([0-9]+)"
	statusPattern           = "status=([a-z]+)"
	masterHostPattern       = "master_host:([0-9.]+)"
	roleMaster              = "role:master"
	defaultRedisPort        = "6379"
	defaultSentinelPort     = "26379"
	masterName              = "mymaster"
	masterReplOffsetPattern = "master_repl_offset:([0-9.]+)"
	runIdPattern            = "run_id:([0-9a-z.]+)"
	connectedSlavesPattern  = "connected_slaves:([0-9]+)"
)

var (
	sentinelsCountRE   = regexp.MustCompile(sentinelsCountPattern)
	slavesCountRE      = regexp.MustCompile(slavesCountPattern)
	statusRE           = regexp.MustCompile(statusPattern)
	masterHostRE       = regexp.MustCompile(masterHostPattern)
	masterReplOffsetRE = regexp.MustCompile(masterReplOffsetPattern)
	runIdRE            = regexp.MustCompile(runIdPattern)
	connectedSlavesRE  = regexp.MustCompile(connectedSlavesPattern)
)

// createRedisOptions creates redis connection options
func createRedisOptions(ip, port, password string) *redis.Options {
	return &redis.Options{
		Addr:     fmt.Sprintf("%s:%s", ip, port),
		Password: password,
		DB:       0,
	}
}

// SentinelCount retrieves the number of sentinels from sentinel memory
func (c *Client) SentinelCount(ctx context.Context, ip, port, password string) (int32, error) {
	client := redis.NewClient(createRedisOptions(ip, port, password))
	defer client.Close()

	info, err := client.Info(ctx, "sentinel").Result()
	if err != nil {
		return 0, err
	}

	if err := checkSentinelStatus(info); err != nil {
		return 0, err
	}

	match := sentinelsCountRE.FindStringSubmatch(info)
	if len(match) == 0 {
		return 0, errors.New("sentinel count regex not found")
	}

	count, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, err
	}
	return int32(count), nil
}

// SlaveCount retrieves the number of slaves from sentinel memory
func (c *Client) SlaveCount(ctx context.Context, ip, port, password string) (int32, error) {
	client := redis.NewClient(createRedisOptions(ip, port, password))
	defer client.Close()

	info, err := client.Info(ctx, "sentinel").Result()
	if err != nil {
		return 0, err
	}

	if err := checkSentinelStatus(info); err != nil {
		return 0, err
	}

	match := slavesCountRE.FindStringSubmatch(info)
	if len(match) == 0 {
		return 0, errors.New("slaves count regex not found")
	}

	count, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, err
	}
	return int32(count), nil
}

// checkSentinelStatus checks if the sentinel is in "ok" status
func checkSentinelStatus(info string) error {
	match := statusRE.FindStringSubmatch(info)
	if len(match) == 0 || match[1] != "ok" {
		return errors.New("sentinel not ready")
	}
	return nil
}

// Reset sends a sentinel reset command
func (c *Client) ResetSentinel(ctx context.Context, ip, port, password string) error {
	client := redis.NewClient(createRedisOptions(ip, port, password))
	defer client.Close()

	cmd := redis.NewIntCmd(ctx, "SENTINEL", "reset", "*")
	client.Process(ctx, cmd)

	if _, err := cmd.Result(); err != nil {
		return err
	}
	return nil
}

func (c *Client) ConnectedSlaves(ctx context.Context, masterIP, port, password string, expected int) (bool, error) {
	client := redis.NewClient(createRedisOptions(masterIP, port, password))
	defer client.Close()

	info, err := client.Info(ctx, "replication").Result()
	if err != nil {
		return false, err
	}

	match := connectedSlavesRE.FindStringSubmatch(info)
	if len(match) == 0 {
		return false, nil
	}

	actual, err := strconv.Atoi(match[1])
	if err != nil {
		return false, err
	}

	return actual == expected, nil
}

// MasterReplicationOffset retrieves the master replication offset
func (c *Client) MasterReplicationOffset(ctx context.Context, ip, port, password string) (int, error) {
	client := redis.NewClient(createRedisOptions(ip, port, password))
	defer client.Close()

	info, err := client.Info(ctx, "replication").Result()
	if err != nil {
		return 0, err
	}

	match := masterReplOffsetRE.FindStringSubmatch(info)
	if len(match) == 0 {
		return 0, nil
	}

	offset, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, err
	}
	return offset, nil
}

// RunID retrieves the Redis instance's run ID
func (c *Client) RunID(ctx context.Context, ip, port, password string) (string, error) {
	client := redis.NewClient(createRedisOptions(ip, port, password))
	defer client.Close()

	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		return "", err
	}

	match := runIdRE.FindStringSubmatch(info)
	if len(match) == 0 {
		return "", nil
	}

	return match[1], nil
}

func (c *Client) IsMaster(ctx context.Context, ip, port, password string) (bool, error) {
	client := redis.NewClient(createRedisOptions(ip, port, password))
	defer client.Close()

	info, err := client.Info(ctx, "replication").Result()
	if err != nil {
		return false, err
	}
	return strings.Contains(info, roleMaster), nil
}

func (c *Client) MonitorRedis(ctx context.Context, ip, port, password string, monitor string, quorum string) error {
	client := redis.NewSentinelClient(createRedisOptions(ip, port, password))
	defer client.Close()

	cmd := redis.NewBoolCmd(ctx, "SENTINEL", "REMOVE", masterName)
	client.Process(ctx, cmd)
	cmd = redis.NewBoolCmd(ctx, "SENTINEL", "MONITOR", masterName, monitor, defaultRedisPort, quorum)
	client.Process(ctx, cmd)
	_, err := cmd.Result()
	if err != nil {
		return err
	}
	return nil
}

// MakeMaster promotes the Redis instance to master
func (c *Client) MakeMaster(ctx context.Context, ip, port, password string) error {
	client := redis.NewClient(createRedisOptions(ip, port, password))
	defer client.Close()

	if res := client.SlaveOf(ctx, "NO", "ONE"); res.Err() != nil {
		return res.Err()
	}
	return nil
}

// MakeSlave connects the Redis instance as a slave to a master
func (c *Client) MakeSlave(ctx context.Context, ip, port, password, masterIP string) error {
	client := redis.NewClient(createRedisOptions(ip, port, password))
	defer client.Close()

	if password != "" {
		if err := client.ConfigSet(ctx, "masterauth", password).Err(); err != nil {
			return err
		}
	}

	if res := client.SlaveOf(ctx, masterIP, port); res.Err() != nil {
		return res.Err()
	}
	return nil
}

func (c *Client) SetPasswd(ctx context.Context, password, ip, port string) error {
	client := redis.NewClient(createRedisOptions(ip, port, password))
	defer client.Close()
	if password != "" {
		if err := client.ConfigSet(ctx, "masterauth", password).Err(); err != nil {
			return err
		}
		if err := client.ConfigSet(ctx, "requirepass", password).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) GetSentinelMonitor(ctx context.Context, ip, port, password string) (string, error) {
	client := redis.NewClient(createRedisOptions(ip, port, password))
	defer client.Close()

	cmd := redis.NewSliceCmd(ctx, "SENTINEL", "master", masterName)
	client.Process(ctx, cmd)
	res, err := cmd.Result()
	if err != nil {
		return "", err
	}
	masterIP := res[3].(string)
	return masterIP, nil
}

func (c *Client) SetCustomSentinelConfig(ctx context.Context, ip, port, password string, configs []string) error {
	client := redis.NewClient(createRedisOptions(ip, port, password))
	defer client.Close()

	if password != "" {
		configs = append(configs, fmt.Sprintf("auth-pass %s", password))
	}

	for _, config := range configs {
		param, value, err := c.getConfigParameters(config)
		if err != nil {
			continue
		}
		if err := c.applySentinelConfig(ctx, param, value, client); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) SetCustomRedisConfig(ctx context.Context, ip, port, password string, configs []string) error {
	client := redis.NewClient(createRedisOptions(ip, port, password))
	defer client.Close()

	for _, config := range configs {
		if config == "" {
			continue
		}
		param, value, err := c.getConfigParameters(config)
		if err != nil {
			return err
		}
		if err := c.applyRedisConfig(ctx, param, value, client); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) SetAOF(ctx context.Context, ip, port, password string, open bool) error {
	client := redis.NewClient(createRedisOptions(ip, port, password))
	defer client.Close()

	v := "no"
	if open {
		v = "yes"
	}

	if err := c.applyRedisConfig(ctx, "appendonly", v, client); err != nil {
		return err
	}
	return nil
}

func (c *Client) applyRedisConfig(ctx context.Context, parameter string, value string, rClient *redis.Client) error {
	result := rClient.ConfigSet(ctx, parameter, value)
	return result.Err()
}

func (c *Client) applySentinelConfig(ctx context.Context, parameter string, value string, rClient *redis.Client) error {
	cmd := redis.NewStatusCmd(ctx, "SENTINEL", "set", masterName, parameter, value)
	rClient.Process(ctx, cmd)
	return cmd.Err()
}

func (c *Client) getConfigParameters(config string) (parameter string, value string, err error) {
	s := strings.Split(config, " ")
	if len(s) < 2 {
		return "", "", fmt.Errorf("configuration '%s' malformed", config)
	}
	return s[0], strings.Join(s[1:], " "), nil
}

func (c *Client) rewriteConfig(ctx context.Context, rClient *redis.Client) error {
	sc := rClient.ConfigRewrite(ctx)
	return sc.Err()
}

func (c *Client) GetRedisStatusFromSentinel(ctx context.Context, ip, port string, password string) (map[string]string, bool, error) {
	m := make(map[string]string)

	sentinelClient := redis.NewSentinelClient(createRedisOptions(ip, port, password))
	defer sentinelClient.Close()
	mi, err := sentinelClient.Master(ctx, "mymaster").Result()
	if err != nil {
		return nil, false, err
	}

	m[mi["ip"]] = mi["role-reported"]

	si, err := sentinelClient.Replicas(ctx, "mymaster").Result()
	if err != nil {
		return nil, false, err
	}

	for _, s := range si {
		m[s["ip"]] = s["role-reported"]
	}

	ck, _ := sentinelClient.CkQuorum(ctx, "mymaster").Result()

	return m, strings.HasPrefix(ck, "OK"), nil
}

func (c *Client) SetSentinelMode(ctx context.Context) error {

}
