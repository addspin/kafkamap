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

type TopicList struct {
	TopicListJson map[string]interface{}
}
type TopicGenerateNewDestPart struct{}
type TopicVerifyNewDestPart struct{}
type TopicApplyNewDestPart struct{}

func (c *TopicList) topicList(client sarama.Client) error {
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		log.Printf("Error creating admin client: %v", err)
		return err
	}
	defer admin.Close()

	// Получаем список топиков
	topics, err := admin.ListTopics()
	if err != nil {
		log.Printf("Error listing topics: %v", err)
		return err
	}

	// Сбор имен топиков
	var topicList []map[string]string
	for topicName := range topics {
		if topicName == "__consumer_offsets" {
			continue
		}
		topicList = append(topicList, map[string]string{"topic": topicName})
	}

	// Сохраняем результат в структуре
	c.TopicListJson = map[string]interface{}{
		"topics":  topicList,
		"version": 1,
	}

	// Преобразуем структуру в JSON для красивого вывода
	jsonData, err := json.MarshalIndent(c.TopicListJson, "", "  ")
	if err != nil {
		log.Printf("Error marshaling JSON: %v", err)
		return err
	}
	log.Printf("Topics list successfully stored in memory:\n%s", string(jsonData))
	return nil
}

func (c *TopicGenerateNewDestPart) topicGenerateNewDestPart() error {
	config := fmt.Sprintf(`security.protocol=%s
sasl.mechanism=%s
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";`, viper.GetString("kafka.sasl.securityProtocol"), viper.GetString("kafka.sasl.mechanism"), viper.GetString("kafka.sasl.username"), viper.GetString("kafka.sasl.password"))

	cmd := exec.Command("docker", "exec", "kafka", "bash", "-c",
		fmt.Sprintf(`echo '%s' > /tmp/config.properties && kafka-topics.sh --describe --bootstrap-server localhost:9092 --command-config /tmp/config.properties && rm /tmp/config.properties`, config))

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

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

func (c *CommandsKafka) TopicVerify(client sarama.Client) error {
	c.topicList.topicList(client)
	c.topicGenerateNewDestPart.topicGenerateNewDestPart()
	// c.topicVerifyNewDestPart.topicVerifyNewDestPart(client)
	return nil

}
