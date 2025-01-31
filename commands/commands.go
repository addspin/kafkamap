package commands

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"

	"github.com/IBM/sarama"
	"github.com/spf13/viper"
)

type TopicApplyReassignPart struct{}

type Topic struct {
	Topics  []map[string]string `json:"topics"`
	Version int                 `json:"version"`
}

func (c *Topic) topicList(client sarama.Client) (*Topic, error) {
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		log.Printf("Ошибка создания клиента: %v", err)
		return nil, err
	}
	defer admin.Close()

	// Получаем список топиков
	topics, err := admin.ListTopics()
	if err != nil {
		log.Printf("Ошибка получения списка топиков: %v", err)
		return nil, err
	}

	// Сбор имен топиков
	var topicList []map[string]string
	for topicName := range topics {
		if topicName == "__consumer_offsets" {
			continue
		}
		topicList = append(topicList, map[string]string{"topic": topicName})
	}

	// Присваиваем значения напрямую полям структуры
	c.Topics = topicList
	c.Version = 1

	// Преобразуем структуру в JSON для красивого вывода
	jsonData, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		log.Printf("Ошибка парсинга JSON: %v", err)
		return nil, err
	}
	log.Printf("Топик лист успешно сохранен в памяти:\n%s", string(jsonData))
	return c, nil
}

func (c *Topic) topicGenerateReassignPart(topicList *Topic) error {
	// Преобраузем список топиков в JSON
	jsonData, err := json.MarshalIndent(topicList, "", "  ")
	if err != nil {
		log.Printf("Ошибка парсинга JSON: %v", err)
		return err
	}

	// Получаем Значения из конфига
	brokerSlice := viper.GetStringSlice("kafka.broker")
	var broker string
	if len(brokerSlice) > 0 {
		broker = brokerSlice[0] // Берем первый элемент из среза
	} else {
		log.Printf("Список брокеров пуст")
		return fmt.Errorf("список брокеров пуст")
	}

	securityProtocol := viper.GetString("kafka.sasl.securityProtocol")
	saslMechanism := viper.GetString("kafka.sasl.mechanism")
	saslUsername := viper.GetString("kafka.sasl.username")
	saslPassword := viper.GetString("kafka.sasl.password")
	brokerListId := viper.GetString("container.brokerList")

	config := fmt.Sprintf(`
	security.protocol=%s
	sasl.mechanism=%s
	sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";`, securityProtocol, saslMechanism, saslUsername, saslPassword)

	commandStr := fmt.Sprintf(`
		echo '%s' > /tmp/config.properties && \
		echo '%s' > /tmp/topics-to-move.json && \
		echo '' > /tmp/all-expand-cluster-reassignment.json && \
		echo '' > /tmp/expand-cluster-reassignment.json && \
		echo '' > /tmp/backup-expand-cluster-reassignment.json && \
		kafka-reassign-partitions.sh --bootstrap-server "%s" \
		--topics-to-move-json-file "/tmp/topics-to-move.json" \
		--broker-list "%s" --generate \
		--command-config /tmp/config.properties > /tmp/all-expand-cluster-reassignment.json && \
		awk '/Current partition replica assignment/,/^$/' /tmp/all-expand-cluster-reassignment.json > /tmp/backup-expand-cluster-reassignment.json && \
		awk '/Proposed partition reassignment configuration/,0' /tmp/all-expand-cluster-reassignment.json > /tmp/expand-cluster-reassignment.json && \
		rm /tmp/config.properties && \
		rm /tmp/topics-to-move.json && \
		rm /tmp/all-expand-cluster-reassignment.json`, config, string(jsonData), broker, brokerListId)

	// Вывод команды в лог
	// log.Printf("Executing command: %s", commandStr)

	cmd := exec.Command("docker", "exec", "kafka", "bash", "-c", commandStr)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func (c *Topic) topicVerifyReassignPart() error {
	brokerSlice := viper.GetStringSlice("kafka.broker")
	var broker string
	if len(brokerSlice) > 0 {
		broker = brokerSlice[0] // Берем первый элемент из среза
	} else {
		log.Printf("Список брокеров пуст")
		return fmt.Errorf("список брокеров пуст")
	}
	securityProtocol := viper.GetString("kafka.sasl.securityProtocol")
	saslMechanism := viper.GetString("kafka.sasl.mechanism")
	saslUsername := viper.GetString("kafka.sasl.username")
	saslPassword := viper.GetString("kafka.sasl.password")

	config := fmt.Sprintf(`
	security.protocol=%s
	sasl.mechanism=%s
	sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";`, securityProtocol, saslMechanism, saslUsername, saslPassword)

	commandStr := fmt.Sprintf(`
		echo '%s' > /tmp/config.properties && \
		kafka-reassign-partitions.sh --bootstrap-server "%s" \
		--reassignment-json-file /tmp/expand-cluster-reassignment.json --verify \
		--command-config /tmp/config.properties && \
		rm /tmp/config.properties`, config, broker)

	// Вывод команды в лог
	// log.Printf("Executing command: %s", commandStr)

	cmd := exec.Command("docker", "exec", "kafka", "bash", "-c", commandStr)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func (c *Topic) topicApplyReassignPart() error {
	brokerSlice := viper.GetStringSlice("kafka.broker")
	var broker string
	if len(brokerSlice) > 0 {
		broker = brokerSlice[0] // Берем первый элемент из среза
	} else {
		log.Printf("Список брокеров пуст")
		return fmt.Errorf("список брокеров пуст")
	}
	securityProtocol := viper.GetString("kafka.sasl.securityProtocol")
	saslMechanism := viper.GetString("kafka.sasl.mechanism")
	saslUsername := viper.GetString("kafka.sasl.username")
	saslPassword := viper.GetString("kafka.sasl.password")

	config := fmt.Sprintf(`
	security.protocol=%s
	sasl.mechanism=%s
	sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";`, securityProtocol, saslMechanism, saslUsername, saslPassword)

	commandStr := fmt.Sprintf(`
		echo '%s' > /tmp/config.properties && \
		kafka-reassign-partitions.sh --bootstrap-server "%s" \
		--reassignment-json-file /tmp/expand-cluster-reassignment.json --execute \
		--command-config /tmp/config.properties && \
		rm /tmp/config.properties`, config, broker)

	// Вывод команды в лог
	// log.Printf("Executing command: %s", commandStr)

	cmd := exec.Command("docker", "exec", "kafka", "bash", "-c", commandStr)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func (c *Topic) topicRollbackReassignPart() error {
	brokerSlice := viper.GetStringSlice("kafka.broker")
	var broker string
	if len(brokerSlice) > 0 {
		broker = brokerSlice[0] // Берем первый элемент из среза
	} else {
		log.Printf("Список брокеров пуст")
		return fmt.Errorf("список брокеров пуст")
	}
	securityProtocol := viper.GetString("kafka.sasl.securityProtocol")
	saslMechanism := viper.GetString("kafka.sasl.mechanism")
	saslUsername := viper.GetString("kafka.sasl.username")
	saslPassword := viper.GetString("kafka.sasl.password")

	config := fmt.Sprintf(`
	security.protocol=%s
	sasl.mechanism=%s
	sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";`, securityProtocol, saslMechanism, saslUsername, saslPassword)

	commandStr := fmt.Sprintf(`
		echo '%s' > /tmp/config.properties && \
		kafka-reassign-partitions.sh --bootstrap-server "%s" \
		--reassignment-json-file /tmp/backup-expand-cluster-reassignment.json --execute \
		--command-config /tmp/config.properties && \
		rm /tmp/config.properties`, config, broker)

	// Вывод команды в лог
	// log.Printf("Executing command: %s", commandStr)

	cmd := exec.Command("docker", "exec", "kafka", "bash", "-c", commandStr)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

// Фасад для команд Kafka
type CommandsKafka struct {
	topic *Topic
}

// Конструктор фасада
func NewCommandKafka() *CommandsKafka {
	return &CommandsKafka{
		topic: &Topic{},
	}
}

func (c *CommandsKafka) TopicGenerateReassignPart(client sarama.Client) error {
	topicList, err := c.topic.topicList(client)
	if err != nil {
		return err
	}
	if err := c.topic.topicGenerateReassignPart(topicList); err != nil {
		return err
	}
	return nil
}

func (c *CommandsKafka) TopicVerify() error {
	if err := c.topic.topicVerifyReassignPart(); err != nil {
		return err
	}
	return nil
}

func (c *CommandsKafka) TopicApply() error {
	if err := c.topic.topicApplyReassignPart(); err != nil {
		return err
	}
	return nil
}

func (c *CommandsKafka) TopicRollback() error {
	if err := c.topic.topicRollbackReassignPart(); err != nil {
		return err
	}
	return nil
}
