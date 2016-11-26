package mqtt_consumer

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
	"strconv"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/parsers"

	"github.com/eclipse/paho.mqtt.golang"
)

type TemplateNode struct {
	tmpl []string
	tn map[string]*TemplateNode
}

type MQTTConsumer struct {
	Servers  []string
	Topics   []string
	Username string
	Password string
	QoS      int `toml:"qos"`
	TopicsTemplates    []string
	TopicsTemplateTree TemplateNode

	parser parsers.Parser

	// Legacy metric buffer support
	MetricBuffer int

	PersistentSession bool
	ClientID          string `toml:"client_id"`

	// Path to CA file
	SSLCA string `toml:"ssl_ca"`
	// Path to host cert file
	SSLCert string `toml:"ssl_cert"`
	// Path to cert key file
	SSLKey string `toml:"ssl_key"`
	// Use SSL but skip chain & host verification
	InsecureSkipVerify bool

	sync.Mutex
	client mqtt.Client
	// channel of all incoming raw mqtt messages
	in   chan mqtt.Message
	done chan struct{}

	// keep the accumulator internally:
	acc telegraf.Accumulator

	started bool
}

var sampleConfig = `
  servers = ["localhost:1883"]
  ## MQTT QoS, must be 0, 1, or 2
  qos = 0

  ## Topics to subscribe to
  topics = [
    "telegraf/host01/cpu",
    "telegraf/+/mem",
    "sensors/#",
  ]

  # if true, messages that can't be delivered while the subscriber is offline
  # will be delivered when it comes back (such as on service restart).
  # NOTE: if true, client_id MUST be set
  persistent_session = false
  # If empty, a random client ID will be generated.
  client_id = ""

  ## username and password to connect MQTT server.
  # username = "telegraf"
  # password = "metricsmetricsmetricsmetrics"

  ## Optional SSL Config
  # ssl_ca = "/etc/telegraf/ca.pem"
  # ssl_cert = "/etc/telegraf/cert.pem"
  # ssl_key = "/etc/telegraf/key.pem"
  ## Use SSL but skip chain & host verification
  # insecure_skip_verify = false

  ## Data format to consume.
  ## Each data format has it's own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
`

func (m *MQTTConsumer) SampleConfig() string {
	return sampleConfig
}

func (m *MQTTConsumer) Description() string {
	return "Read metrics from MQTT topic(s)"
}

func (m *MQTTConsumer) SetParser(parser parsers.Parser) {
	m.parser = parser
}

func (m *MQTTConsumer) Start(acc telegraf.Accumulator) error {
	m.Lock()
	defer m.Unlock()
	m.started = false

	err := m.prepTopicsTemplateTree()
	if err != nil {
		return err
	}

	if m.PersistentSession && m.ClientID == "" {
		return fmt.Errorf("ERROR MQTT Consumer: When using persistent_session" +
			" = true, you MUST also set client_id")
	}

	m.acc = acc
	if m.QoS > 2 || m.QoS < 0 {
		return fmt.Errorf("MQTT Consumer, invalid QoS value: %d", m.QoS)
	}

	opts, err := m.createOpts()
	if err != nil {
		return err
	}

	m.client = mqtt.NewClient(opts)
	if token := m.client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	m.in = make(chan mqtt.Message, 1000)
	m.done = make(chan struct{})

	go m.receiver()

	return nil
}
func (m *MQTTConsumer) onConnect(c mqtt.Client) {
	log.Printf("I! MQTT Client Connected")
	if !m.PersistentSession || !m.started {
		topics := make(map[string]byte)
		for _, topic := range m.Topics {
			topics[topic] = byte(m.QoS)
		}
		subscribeToken := c.SubscribeMultiple(topics, m.recvMessage)
		subscribeToken.Wait()
		if subscribeToken.Error() != nil {
			log.Printf("E! MQTT Subscribe Error\ntopics: %s\nerror: %s",
				strings.Join(m.Topics[:], ","), subscribeToken.Error())
		}
		m.started = true
	}
	return
}

func (m *MQTTConsumer) onConnectionLost(c mqtt.Client, err error) {
	log.Printf("E! MQTT Connection lost\nerror: %s\nMQTT Client will try to reconnect", err.Error())
	return
}

// receiver() reads all incoming messages from the consumer, and parses them into
// influxdb metric points.
func (m *MQTTConsumer) receiver() {
	for {
		select {
		case <-m.done:
			return
		case msg := <-m.in:
			topic := msg.Topic()
<<<<<<< HEAD
			payload := msg.Payload()

			if len(m.TopicsTemplates) > 0 {
				tlns := strings.Split(topic, "/")
				tmpls, tmpl_found := m.getTemplateForTopic(tlns)
				if tmpl_found {
					for _, tmpl := range tmpls {
						a_payload, _ := rewr(tlns, payload, tmpl)
						metric, err := m.parser.ParseLine(a_payload)
						if err != nil {
							log.Printf("E! MQTT Parse Error\nmessage: %s\nerror: %s",
								string(a_payload), err.Error())
						} else {
							m.acc.AddFields(metric.Name(), metric.Fields(), metric.Tags(), metric.Time())
						}
					}
				}
			} else {
				metrics, err := m.parser.Parse(payload)
				if err != nil {
					log.Printf("E! MQTT Parse Error\nmessage: %s\nerror: %s",
						string(payload), err.Error())
				}

				for _, metric := range metrics {
					tags := metric.Tags()
					tags["topic"] = topic
					m.acc.AddFields(metric.Name(), metric.Fields(), tags, metric.Time())
				}
			}
		}
	}
}

func getTopicNodeFromIdx(topic *[]string, idx int) (string, bool) {
	t := *topic
	if idx == 0 {
		idx = len(t) - 1
	} else if idx < 0 {
		idx = len(t) - 1 + idx
	} else {
		idx--;
	}
	if idx > (len(t) - 1) {
		return "", false
	}

	return t[idx], true
}

func rewr(topic []string, val []byte, tmpl string) (string, bool) {
	s := strings.FieldsFunc(tmpl, func(c rune) bool {
		return c == '{' || c == '}'
	})

	for i := 1; i < len(s); i++ {
		idx, err := strconv.Atoi(s[i])
		if err == nil {
			res, resOk := getTopicNodeFromIdx(&topic, idx)
			if resOk {
				s[i] = res
			} else {
				return "", false
			}
		} else if s[i] == "v" {
			s[i] = string(val)
		} else {
			sa := strings.Split(s[i], ",")
			res := ""
			if len(sa) == 2 {
				kIdx, err := strconv.Atoi(sa[0])
				if err != nil && sa[0] == "k" {
					kIdx = len(topic) - 1
					err = nil
				}
				if err == nil {
					kTn, resOk := getTopicNodeFromIdx(&topic, kIdx)
					if resOk {
						vTn := ""
						resOk = true
						vIdx, err := strconv.Atoi(sa[1])
						if err == nil {
							vTn, resOk = getTopicNodeFromIdx(&topic, vIdx)
						} else if sa[1] == "v" {
							vTn = string(val)
							err = nil
						}
						if resOk {
							ks := strings.Split(kTn, ",")
							vs := strings.Split(vTn, ",")
							if len(ks) == len(vs) {
								ts := make([]string, len(ks))

								for i, key := range ks {
									ts[i] = strings.TrimSpace(key) + "=" + strings.TrimSpace(vs[i])
								}
								res = strings.Join(ts, ",")
							}
						}
					}
				}
=======
			metrics, err := m.parser.Parse(msg.Payload())
			if err != nil {
				log.Printf("E! MQTT Parse Error\nmessage: %s\nerror: %s",
					string(msg.Payload()), err.Error())
>>>>>>> c03661712cdb9d5fe8329f16c53fb1186f406e14
			}
			s[i] = res
		}
		i++
	}

	return strings.Join(s, ""), true
}

func (m *MQTTConsumer) prepTopicsTemplateTree() error {
	m.TopicsTemplateTree.tn = make(map[string]*TemplateNode)
	for _, template := range m.TopicsTemplates {
		tmpl := strings.Split(template, ";")
		tlns := strings.Split(tmpl[0], "/")
		m.addTopicFilterTemplate(&m.TopicsTemplateTree, tlns, tmpl[1:])
	}
	return nil
}

func (m *MQTTConsumer) addTopicFilterTemplate(topicNode *TemplateNode, tlns []string, template []string) error {
	if len(tlns) > 0 {
		tln := tlns[0]
		if len(tln) == 0 {
			tln = "/"
		}
		tn, prs := topicNode.tn[tln]

		if !prs {
			tn = new(TemplateNode)
			tn.tn = make(map[string]*TemplateNode)
			topicNode.tn[tln] = tn
		}

		m.addTopicFilterTemplate(tn, tlns[1:],template)
	} else {
		topicNode.tmpl = template;
	}
	return nil
}

func (m *MQTTConsumer) findTemplateForTopic(topicNode *TemplateNode, tlns []string) ([]string, bool) {
	if len(tlns) > 0 {
		tln := tlns[0]
		if len(tln) == 0 {
			tln = "/"
		}
		tn, prs := topicNode.tn[tln]
		if !prs {
			tn, prs = topicNode.tn["+"]
		}
		if prs {
			tmpl, res := m.findTemplateForTopic(tn, tlns[1:])
			if len(tmpl) > 0 {
				return tmpl, res
			}
		}
		tn, prs = topicNode.tn["#"]
		if prs {
			return tn.tmpl, true
		} else {
			return make([]string, 0), false
		}
	} else {
		return topicNode.tmpl, true
	}
}

func (m *MQTTConsumer) getTemplateForTopic(tlns []string) ([]string, bool) {
	tmpl, res := m.findTemplateForTopic(&m.TopicsTemplateTree, tlns)
	return tmpl, res
}

func (m *MQTTConsumer) recvMessage(_ mqtt.Client, msg mqtt.Message) {
	m.in <- msg
}

func (m *MQTTConsumer) Stop() {
	m.Lock()
	defer m.Unlock()
	close(m.done)
	m.client.Disconnect(200)
	m.started = false
}

func (m *MQTTConsumer) Gather(acc telegraf.Accumulator) error {
	return nil
}

func (m *MQTTConsumer) createOpts() (*mqtt.ClientOptions, error) {
	opts := mqtt.NewClientOptions()

	if m.ClientID == "" {
		opts.SetClientID("Telegraf-Consumer-" + internal.RandomString(5))
	} else {
		opts.SetClientID(m.ClientID)
	}

	tlsCfg, err := internal.GetTLSConfig(
		m.SSLCert, m.SSLKey, m.SSLCA, m.InsecureSkipVerify)
	if err != nil {
		return nil, err
	}

	scheme := "tcp"
	if tlsCfg != nil {
		scheme = "ssl"
		opts.SetTLSConfig(tlsCfg)
	}

	user := m.Username
	if user != "" {
		opts.SetUsername(user)
	}
	password := m.Password
	if password != "" {
		opts.SetPassword(password)
	}

	if len(m.Servers) == 0 {
		return opts, fmt.Errorf("could not get host infomations")
	}
	for _, host := range m.Servers {
		server := fmt.Sprintf("%s://%s", scheme, host)

		opts.AddBroker(server)
	}
	opts.SetAutoReconnect(true)
	opts.SetKeepAlive(time.Second * 60)
	opts.SetCleanSession(!m.PersistentSession)
	opts.SetOnConnectHandler(m.onConnect)
	opts.SetConnectionLostHandler(m.onConnectionLost)
	return opts, nil
}

func init() {
	inputs.Add("mqtt_consumer", func() telegraf.Input {
		return &MQTTConsumer{}
	})
}

