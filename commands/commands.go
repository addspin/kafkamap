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

// Фасад для команд Kafka
type CommandsKafka struct {
	topicList                *TopicList
	topicGenerateNewDestPart *TopicGenerateNewDestPart
	topicVerifyNewDestPart   *TopicVerifyNewDestPart
	topicApplyNewDestPart    *TopicApplyNewDestPart
}

// Конструктор фасада
func NewCommandKafka() *CommandsKafka {
	return &CommandsKafka{
		topicList:                &TopicList{},
		topicGenerateNewDestPart: &TopicGenerateNewDestPart{},
		topicVerifyNewDestPart:   &TopicVerifyNewDestPart{},
		topicApplyNewDestPart:    &TopicApplyNewDestPart{},
	}
}

type TopicList struct {
	Topics  []map[string]string `json:"topics"`
	Version int                 `json:"version"`
}

type TopicGenerateNewDestPart struct{}
type TopicVerifyNewDestPart struct{}
type TopicApplyNewDestPart struct{}

func (c *TopicList) topicList(client sarama.Client) (*TopicList, error) {
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		log.Printf("Error creating admin client: %v", err)
		return nil, err
	}
	defer admin.Close()

	// Получаем список топиков
	topics, err := admin.ListTopics()
	if err != nil {
		log.Printf("Error listing topics: %v", err)
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
		log.Printf("Error marshaling JSON: %v", err)
		return nil, err
	}
	log.Printf("Topics list successfully stored in memory:\n%s", string(jsonData))
	return c, nil

}

func (c *TopicGenerateNewDestPart) topicGenerateNewDestPart(topicList *TopicList) error {
	// Преобраузем список топиков в JSON
	jsonData, err := json.MarshalIndent(topicList, "", "  ")
	if err != nil {
		log.Printf("Error marshaling JSON: %v", err)
		return err
	}

	config := fmt.Sprintf(`
	security.protocol=%s
	sasl.mechanism=%s
	sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";`, viper.GetString("kafka.sasl.securityProtocol"), viper.GetString("kafka.sasl.mechanism"), viper.GetString("kafka.sasl.username"), viper.GetString("kafka.sasl.password"))

	cmd := exec.Command("docker", "exec", "kafka", "bash", "-c",
		fmt.Sprintf(`
		echo '%s' > /tmp/config.properties && kafka-topics.sh --describe --bootstrap-server "%s" --topics-to-move-json-file "%s" --broker-list "%s" --generate
		Current partition replica assignment --command-config /tmp/config.properties && rm /tmp/config.properties`, config, viper.GetString("kafka.broker"), string(jsonData), viper.GetString("kafka.brokerList")))

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func (c *CommandsKafka) TopicVerify(client sarama.Client) error {
	topicList, err := c.topicList.topicList(client)
	if err != nil {
		return err
	}
	c.topicGenerateNewDestPart.topicGenerateNewDestPart(topicList)
	// c.topicVerifyNewDestPart.topicVerifyNewDestPart(client)
	return nil

}
