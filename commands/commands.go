package commands

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"slices"
	"strings"

	"github.com/IBM/sarama"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

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

	return c, nil
}

// Получаем список свободных id реплик (брокеров) для каждого топика
func (c *Topic) whoTopicPart(client sarama.Client, configFile string, brokerIDs []int32) (map[string][]int32, map[string][]int32, error) {

	type TopicConfig struct {
		CleanupPolicy     string `yaml:"cleanup.policy"`
		MaxMessageBytes   int    `yaml:"max.message.bytes"`
		MinInsyncReplicas int    `yaml:"min.insync.replicas"`
		RetentionBytes    int    `yaml:"retention.bytes"`
		RetentionMs       int    `yaml:"retention.ms"`
		SegmentBytes      int    `yaml:"segment.bytes"`
		Partitions        int    `yaml:"partitions"`
		Replicas          int    `yaml:"replicas"`
	}

	// Чтение YAML файла
	data, err := os.ReadFile(configFile)
	if err != nil {
		log.Printf("Ошибка чтения файла %s: %v", configFile, err)
		return nil, nil, err
	}

	// TopicsFile представляет структуру YAML файла с топиками
	type TopicsFile struct {
		Topics map[string]TopicConfig `yaml:"topics"`
	}

	// Парсинг YAML в карту
	var topicsFile TopicsFile
	err = yaml.Unmarshal(data, &topicsFile)
	if err != nil {
		log.Printf("Ошибка парсинга YAML файла: %v", err)
		return nil, nil, err
	}
	// Проверка наличия секции "topics"
	topicsConfig := topicsFile.Topics
	if len(topicsConfig) == 0 {
		return nil, nil, fmt.Errorf("отсутствует секция 'topics'")
	}

	// Создаем админ-клиент
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		log.Printf("Ошибка создания админ-клиента: %v", err)
		return nil, nil, err
	}
	defer admin.Close()

	// Получаем список топиков из kafka
	topics, err := admin.ListTopics()
	if err != nil {
		log.Printf("Ошибка получения списка топиков: %v", err)
		return nil, nil, err
	}

	topicList := make([]string, 0)

	// Заполняем список топиков из kafka
	for topicName := range topics {
		if topicName == "__consumer_offsets" {
			continue
		}
		topicList = append(topicList, topicName)
		// log.Printf("Топик в kafka: %s", topicName)
	}

	// Создаем результирующую мапу для id реплик (брокеров) для каждого топика
	replicaBrokerId := make(map[string][]int32)
	// Создаем результирующую мапу для id новых брокеров для каждого топика
	freeReplicaBrokerId := make(map[string][]int32)
	// Для каждого топика из файла конфигурации проверяем текущее колличество брокеров для реплики
	// и то что указано в конфигурации
	for topicNameConfig := range topicsConfig {
		log.Printf("Топик в конфиге: %s", topicNameConfig)
		// Получаем метаданные топика
		metadata, err := admin.DescribeTopics([]string{topicNameConfig})
		if err != nil {
			log.Printf("Ошибка получения метаданных для топика %s: %v", topicNameConfig, err)
			continue
		}

		// Проверяем наличие топиков которые указаны в конфигурации и их наличие в kafka

		// log.Printf("Топик в kafka: %s", topic)
		if !slices.Contains(topicList, topicNameConfig) {
			return nil, nil, fmt.Errorf("топик %s отсутствует в Kafka", topicNameConfig)

		}

		if len(metadata) > 0 {
			var replicaBrokerID []int32

			// Собираем ID брокеров с 0 партиции
			for _, partition := range metadata[0].Partitions {
				replicaBrokerID = partition.Replicas

				log.Printf("Текущие реплики на брокерах: %v", replicaBrokerID)
			}
			// Если в кластере то же значение реплик что и в конфиге или больше то выводим ошибку
			if len(replicaBrokerID) >= topicsConfig[topicNameConfig].Replicas {
				log.Printf("Длина: %d", len(replicaBrokerID))
				return nil, nil, fmt.Errorf("недостаточно брокеров для репликации: имеется %d, требуется больше чем %d", len(replicaBrokerID), topicsConfig[topicNameConfig].Replicas)
			} else {
				replicaBrokerId[topicNameConfig] = replicaBrokerID
				// Сравниваем id брокеров в кластере с id брокерами топика, которые свободны в кластере добавляем в мапу
				for _, id := range brokerIDs {
					if !slices.Contains(replicaBrokerID, id) {
						freeReplicaBrokerId[topicNameConfig] = append(freeReplicaBrokerId[topicNameConfig], id)

					}
				}
				log.Printf("Неиспользуемые брокеры для реплик: %d", freeReplicaBrokerId[topicNameConfig])
			}
		}
	}
	return freeReplicaBrokerId, replicaBrokerId, nil
}

// требуется дописать
func (c *Topic) topicGenerateReplacationFactor(configFile string, freeReplicaBrokerId map[string][]int32, replicaBrokerId map[string][]int32) error {

	// type TopicConfig struct {
	// 	CleanupPolicy     string `yaml:"cleanup.policy"`
	// 	MaxMessageBytes   int    `yaml:"max.message.bytes"`
	// 	MinInsyncReplicas int    `yaml:"min.insync.replicas"`
	// 	RetentionBytes    int    `yaml:"retention.bytes"`
	// 	RetentionMs       int    `yaml:"retention.ms"`
	// 	SegmentBytes      int    `yaml:"segment.bytes"`
	// 	Partitions        int    `yaml:"partitions"`
	// 	Replicas          int    `yaml:"replicas"`
	// }

	// // Чтение YAML файла
	// data, err := os.ReadFile(configFile)
	// if err != nil {
	// 	log.Printf("Ошибка чтения файла %s: %v", configFile, err)
	// 	return err
	// }

	// // TopicsFile представляет структуру YAML файла с топиками
	// type TopicsFile struct {
	// 	Topics map[string]TopicConfig `yaml:"topics"`
	// }

	// // Парсинг YAML в карту
	// var topicsFile TopicsFile
	// err = yaml.Unmarshal(data, &topicsFile)
	// if err != nil {
	// 	log.Printf("Ошибка парсинга YAML файла: %v", err)
	// 	return err
	// }

	// // Проверка наличия секции "topics"
	// topicsConfig := topicsFile.Topics
	// if len(topicsConfig) == 0 {
	// 	return fmt.Errorf("отсутствует секция 'topics'")
	// }

	// topicList := make([]string, 0)

	// Инициализируем структуру Topic
	topicList := &Topic{
		Version: 1,
		Topics:  make([]map[string]string, 0),
	}
	for topicName := range replicaBrokerId {
		topicList.Topics = append(topicList.Topics, map[string]string{"topic": topicName})
	}

	// for topicName := range topicsConfig {
	// 	topicList = append(topicList, topicName)
	// }

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

	// Сравнимаем текущие брокер-реплики с фактором репликации

	securityProtocol := viper.GetString("kafka.sasl.securityProtocol")
	saslMechanism := viper.GetString("kafka.sasl.mechanism")
	saslUsername := viper.GetString("kafka.sasl.username")
	saslPassword := viper.GetString("kafka.sasl.password")

	config := fmt.Sprintf(`
	security.protocol=%s
	sasl.mechanism=%s
	sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";`, securityProtocol, saslMechanism, saslUsername, saslPassword)

	for _, replicaBrokersIDExists := range replicaBrokerId {
		// подготовка json для увеличения репликации с текущими репликами
		commandStr := fmt.Sprintf(`
		echo '%s' > /tmp/config.properties && \
		echo '%s' > /tmp/replication-factor.json && \
		RESULT=$(kafka-reassign-partitions.sh --bootstrap-server "%s" \
		--topics-to-move-json-file "/tmp/replication-factor.json" \
		--broker-list "%s" --generate \
		--command-config /tmp/config.properties)
		if [ $? -ne 0 ]; then
			echo $RESULT >&2
			rm /tmp/config.properties /tmp/replication-factor.json
			exit 1
		fi && \
		echo "$RESULT" | awk '
			/Proposed partition reassignment configuration/,0 {
				if (!/Proposed partition reassignment configuration/) {
					print > "/tmp/increase-replication-factor.json"
				}
			}
		' && \
		rm /tmp/config.properties`, config, string(jsonData), broker, replicaBrokersIDExists)

		cmd := exec.Command("docker", "exec", "kafka", "bash", "-c", commandStr)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("%s", string(output))
		}
	}
	for _, freeReplicaBrokerId := range freeReplicaBrokerId {
		// подготовка json для увеличения репликации с новыми брокерами
		commandFinalStr := fmt.Sprintf(`
		echo '%s' > /tmp/config.properties && \
		echo '%s' > /tmp/replication-factor.json && \
		RESULT=$(kafka-reassign-partitions.sh --bootstrap-server "%s" \
		--reassignment-json-file "/tmp/increase-replication-factor.json" \
		--broker-list "%s" --generate \
		--command-config /tmp/config.properties)
		if [ $? -ne 0 ]; then
			echo $RESULT >&2
			rm /tmp/config.properties /tmp/replication-factor.json
			exit 1
		fi && \
		echo "$RESULT" | awk '
			/Proposed partition reassignment configuration/,0 {
				if (!/Proposed partition reassignment configuration/) {
					print > "/tmp/increase-replication-factor.json"
				}
			}
		' && \
		rm /tmp/config.properties`, config, string(jsonData), broker, freeReplicaBrokerId)

		cmdFinal := exec.Command("docker", "exec", "kafka", "bash", "-c", commandFinalStr)
		outputFinal, errFinal := cmdFinal.CombinedOutput()
		if errFinal != nil {
			return fmt.Errorf("%s", string(outputFinal))
		}

	}
	return nil
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
		RESULT=$(kafka-reassign-partitions.sh --bootstrap-server "%s" \
		--topics-to-move-json-file "/tmp/topics-to-move.json" \
		--broker-list "%s" --generate \
		--command-config /tmp/config.properties)
		if [ $? -ne 0 ]; then
			echo $RESULT >&2
			rm /tmp/config.properties /tmp/topics-to-move.json
			exit 1
		fi && \
		echo "$RESULT" | awk '
			/Current partition replica assignment/,/^$/ {
				if (!/Current partition replica assignment/ && !/^$/) {
					print > "/tmp/backup-expand-cluster-reassignment.json"
				}
			}
			/Proposed partition reassignment configuration/,0 {
				if (!/Proposed partition reassignment configuration/) {
					print > "/tmp/expand-cluster-reassignment.json"
				}
			}
		' && \
		rm /tmp/config.properties`, config, string(jsonData), broker, brokerListId)

	cmd := exec.Command("docker", "exec", "kafka", "bash", "-c", commandStr)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s", string(output))
	}
	return nil
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

func (c *Topic) topicCreate(filePath string) error {
	// Чтение YAML файла
	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Printf("Ошибка чтения файла %s: %v", filePath, err)
		return err
	}

	// Парсинг YAML в карту
	var values map[string]map[string]map[string]interface{}
	err = yaml.Unmarshal(data, &values)
	if err != nil {
		log.Printf("Ошибка парсинга YAML файла: %v", err)
		return err
	}

	// Проверка наличия секции "topics"
	topicsConfig, exists := values["topics"]
	if !exists {
		return fmt.Errorf("отсутствует секция 'topics'")
	}

	// Получаем параметры из конфига
	brokerSlice := viper.GetStringSlice("kafka.broker")
	var broker string
	if len(brokerSlice) > 0 {
		broker = brokerSlice[0]
	} else {
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

	// Создаем топики
	// var createErrors []string
	for topicName, params := range topicsConfig {
		var configParams []string
		var partitions, replicas string

		// Обработка параметров
		for key, value := range params {
			strValue := fmt.Sprintf("%v", value)
			switch key {
			case "partitions":
				partitions = strValue
			case "replicas":
				replicas = strValue
			default:
				configParams = append(configParams, fmt.Sprintf("%s=%s", key, strValue))
			}
		}

		// Формируем дополнительные параметры конфигурации идущие через --config
		var command string
		if len(configParams) > 0 {
			for _, param := range configParams {
				command += fmt.Sprintf(` --config %s`, param)
			}
		}

		// Формируем команду
		commandStr := fmt.Sprintf(`
			echo '%s' > /tmp/config.properties && \
			kafka-topics.sh --create --topic %s \
			--bootstrap-server %s \
			--partitions %s \
			--replication-factor %s \
			%s \
			--command-config /tmp/config.properties && \
			rm /tmp/config.properties`,
			config, topicName, broker, partitions, replicas, command)

		// Выполняем команду
		cmd := exec.Command("docker", "exec", "kafka", "bash", "-c", commandStr)
		cmd.Stdout = os.Stdout
		// cmd.Stderr = os.Stderr // Выводим только ошибки инструментов кафки

		if err := cmd.Run(); err != nil {
			continue // Продолжаем со следующим топиком
		}
		log.Printf("Топик %s успешно создан", topicName)
	}
	return nil
}

type Broker struct{}

// func (b *Broker) brokerList(client sarama.Client) ([]int32, error) {
// 	// Получаем список всех брокеров из кластера
// 	brokers := client.Brokers()
// 	if len(brokers) == 0 {
// 		return []int32{}, fmt.Errorf("не найдено активных брокеров")
// 	}

// 	// Получаем ID каждого брокера
// 	var brokerIDs []int32
// 	for _, broker := range brokers {
// 		brokerIDs = append(brokerIDs, broker.ID())
// 	}
// 	return brokerIDs, nil
// }

type Acl struct{}

func (a *Acl) aclList(principal string) error {
	// Получаем параметры из конфига
	brokerSlice := viper.GetStringSlice("kafka.broker")
	var broker string
	if len(brokerSlice) > 0 {
		broker = brokerSlice[0]
	} else {
		return fmt.Errorf("список брокеров пуст")
	}

	securityProtocol := viper.GetString("kafka.sasl.securityProtocol")
	saslMechanism := viper.GetString("kafka.sasl.mechanism")
	saslAdminUsername := viper.GetString("kafka.sasl.username")
	saslAdminPassword := viper.GetString("kafka.sasl.password")

	config := fmt.Sprintf(`
	security.protocol=%s
	sasl.mechanism=%s
	sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";`, securityProtocol, saslMechanism, saslAdminUsername, saslAdminPassword)

	// Базовая часть команды
	baseCommand := fmt.Sprintf(`
		echo '%s' > /tmp/config.properties && \
		echo "===== Список всех ACL =====" && \
		kafka-acls.sh --bootstrap-server %s \
		--command-config /tmp/config.properties \
		--list`, config, broker)

	principalFilter := ""
	if principal != "" {
		principalFilter = fmt.Sprintf(` --principal "User:%s"`, principal)
	}

	// Команда для отображения всех ACL без фильтров по типу ресурса
	commandStr := fmt.Sprintf(`%s%s`, baseCommand, principalFilter)

	// Завершаем команду
	commandStr += ` && rm /tmp/config.properties`

	// Выполняем команду
	cmd := exec.Command("docker", "exec", "kafka", "bash", "-c", commandStr)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("ошибка получения списка ACL: %v", err)
	}

	return nil
}

type User struct{}

func (u *User) userCreate(filePath string) error {
	// Получаем параметры из конфига
	brokerSlice := viper.GetStringSlice("kafka.broker")
	var broker string
	if len(brokerSlice) > 0 {
		broker = brokerSlice[0]
	} else {
		return fmt.Errorf("список брокеров пуст")
	}

	securityProtocol := viper.GetString("kafka.sasl.securityProtocol")
	saslMechanism := viper.GetString("kafka.sasl.mechanism")
	saslAdminUsername := viper.GetString("kafka.sasl.username")
	saslAdminPassword := viper.GetString("kafka.sasl.password")

	config := fmt.Sprintf(`
	security.protocol=%s
	sasl.mechanism=%s
	sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";`, securityProtocol, saslMechanism, saslAdminUsername, saslAdminPassword)

	// Чтение данных пользователей из YAML-файла
	type User struct {
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	}

	type UsersFile struct {
		Users []User `yaml:"users"`
	}

	// Чтение YAML файла
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("ошибка чтения файла %s: %v", filePath, err)
	}

	// Парсинг YAML в структуру
	var usersFile UsersFile
	err = yaml.Unmarshal(data, &usersFile)
	if err != nil {
		return fmt.Errorf("ошибка парсинга YAML файла: %v", err)
	}

	// Проверка наличия пользователей
	if len(usersFile.Users) == 0 {
		return fmt.Errorf("не найдено пользователей в файле")
	}

	// Создаем пользователей из файла
	for _, user := range usersFile.Users {
		if user.Username == "" || user.Password == "" {
			log.Printf("Пропуск пользователя из-за отсутствия имени или пароля")
			continue
		}

		// Формируем команду для создания пользователя
		commandStr := fmt.Sprintf(`
			echo '%s' > /tmp/config.properties && \
			kafka-configs.sh --bootstrap-server %s \
			--command-config /tmp/config.properties \
			--alter --add-config 'SCRAM-SHA-512=[password=%s]' \
			--entity-type users --entity-name %s && \
			rm /tmp/config.properties`,
			config, broker, user.Password, user.Username)

		// Выполняем команду
		cmd := exec.Command("docker", "exec", "kafka", "bash", "-c", commandStr)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			log.Printf("Ошибка создания пользователя %s: %v", user.Username, err)
			continue // Продолжаем со следующим пользователем
		}

		log.Printf("Пользователь %s успешно создан", user.Username)
	}

	return nil
}

func (u *User) userAddAcl(filePath string) error {
	// Получаем параметры из конфига
	brokerSlice := viper.GetStringSlice("kafka.broker")
	var broker string
	if len(brokerSlice) > 0 {
		broker = brokerSlice[0]
	} else {
		return fmt.Errorf("список брокеров пуст")
	}

	securityProtocol := viper.GetString("kafka.sasl.securityProtocol")
	saslMechanism := viper.GetString("kafka.sasl.mechanism")
	saslAdminUsername := viper.GetString("kafka.sasl.username")
	saslAdminPassword := viper.GetString("kafka.sasl.password")

	config := fmt.Sprintf(`
	security.protocol=%s
	sasl.mechanism=%s
	sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";`, securityProtocol, saslMechanism, saslAdminUsername, saslAdminPassword)

	// Структуры для YAML файла
	type Acl struct {
		Allow               bool   `yaml:"allow"`
		Operation           string `yaml:"operation"`
		Topic               string `yaml:"topic,omitempty"`
		Group               string `yaml:"group,omitempty"`
		Cluster             string `yaml:"cluster,omitempty"`
		ResourcePatternType string `yaml:"resource-pattern-type"`
	}

	type User struct {
		Username string `yaml:"username"`
		Password string `yaml:"password"`
		Acls     []Acl  `yaml:"acls,omitempty"`
	}

	type UsersFile struct {
		Users []User `yaml:"users"`
	}

	// Чтение YAML файла
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("ошибка чтения файла %s: %v", filePath, err)
	}

	// Парсинг YAML в структуру
	var usersFile UsersFile
	err = yaml.Unmarshal(data, &usersFile)
	if err != nil {
		return fmt.Errorf("ошибка парсинга YAML файла: %v", err)
	}

	// Проверка наличия пользователей
	if len(usersFile.Users) == 0 {
		return fmt.Errorf("не найдено пользователей в файле")
	}

	// Обработка ACL правил для каждого пользователя
	for _, user := range usersFile.Users {
		if user.Username == "" {
			log.Printf("Пропуск пользователя из-за отсутствия имени")
			continue
		}

		principal := fmt.Sprintf("User:%s", user.Username)

		// Обрабатываем все ACL для текущего пользователя
		for _, acl := range user.Acls {
			// Определяем тип операции (по умолчанию все в верхнем регистре)
			operation := strings.ToUpper(acl.Operation)

			// Определяем тип доступа (allow/deny)
			accessType := "allow"
			if !acl.Allow {
				accessType = "deny"
			}

			// Определяем тип шаблона ресурса
			resourcePatternType := strings.ToLower(acl.ResourcePatternType)

			// Формируем команду в зависимости от типа ресурса
			var resourceType, resourceName string
			if acl.Topic != "" {
				resourceType = "topic"
				resourceName = acl.Topic
			} else if acl.Group != "" {
				resourceType = "group"
				resourceName = acl.Group
			} else if acl.Cluster != "" {
				resourceType = "cluster"
				resourceName = acl.Cluster
			} else {
				log.Printf("Пропуск ACL: не указан ресурс")
				continue
			}

			// Формируем команду для добавления ACL
			commandStr := fmt.Sprintf(`
				echo '%s' > /tmp/config.properties && \
				kafka-acls.sh --bootstrap-server %s \
				--command-config /tmp/config.properties \
				--add \
				--%s-principal "%s" \
				--operation %s \
				--resource-pattern-type %s \
				--%s "%s" && \
				rm /tmp/config.properties`,
				config, broker, accessType, principal, operation, resourcePatternType, resourceType, resourceName)

			// Выполняем команду
			cmd := exec.Command("docker", "exec", "kafka", "bash", "-c", commandStr)
			output, err := cmd.CombinedOutput()
			if err != nil {
				log.Printf("Ошибка добавления ACL для пользователя %s: %v\n%s", user.Username, err, string(output))
				continue // Продолжаем со следующим ACL
			}

			log.Printf("ACL успешно добавлен для пользователя %s: %s %s на %s:%s",
				user.Username, operation, accessType, resourceType, resourceName)
		}
	}

	return nil
}

// Фасад для команд Kafka
type CommandsKafka struct {
	topic  *Topic
	acl    *Acl
	broker *Broker
	user   *User
}

// Конструктор фасада
func NewCommandKafka() *CommandsKafka {
	return &CommandsKafka{
		topic:  &Topic{},
		acl:    &Acl{},
		broker: &Broker{},
		user:   &User{},
	}
}

func (c *CommandsKafka) TopicGenerateReassignPart(client sarama.Client, topicsFile string) error {
	if topicsFile != "" {
		// Читаем топики из файла
		content, err := os.ReadFile(topicsFile)
		if err != nil {
			return fmt.Errorf("ошибка чтения файла с топиками: %v", err)
		}

		// Инициализируем структуру Topic
		topicList := &Topic{
			Version: 1,
			Topics:  make([]map[string]string, 0),
		}

		// Разбираем топики из файла
		topicNames := strings.Split(strings.TrimSpace(string(content)), "\n")
		for _, topicName := range topicNames {
			if topicName != "" {
				topicList.Topics = append(topicList.Topics, map[string]string{"topic": topicName})
			}
		}

		if err := c.topic.topicGenerateReassignPart(topicList); err != nil {
			return err
		}
	} else {
		// Получаем все топики, если файл не указан
		topicList, err := c.topic.topicList(client)
		if err != nil {
			return err
		}
		if err := c.topic.topicGenerateReassignPart(topicList); err != nil {
			return err
		}
	}

	return nil
}

func (c *CommandsKafka) TopicVerifyReassignPart() error {
	if err := c.topic.topicVerifyReassignPart(); err != nil {
		return err
	}
	return nil
}

func (c *CommandsKafka) TopicApplyReassignPart() error {
	if err := c.topic.topicApplyReassignPart(); err != nil {
		return err
	}
	return nil
}

func (c *CommandsKafka) TopicRollbackReassignPart() error {
	if err := c.topic.topicRollbackReassignPart(); err != nil {
		return err
	}
	return nil
}

func (c *CommandsKafka) TopicCreate(filePath string) error {
	if err := c.topic.topicCreate(filePath); err != nil {
		return err
	}
	return nil
}

func (c *CommandsKafka) UserCreate(filePath string) error {
	if err := c.user.userCreate(filePath); err != nil {
		return err
	}
	return nil
}

func (c *CommandsKafka) UserAddAcl(filePath string) error {
	if err := c.user.userAddAcl(filePath); err != nil {
		return err
	}
	return nil
}

func (c *CommandsKafka) AclList(principal string) error {
	if err := c.acl.aclList(principal); err != nil {
		return err
	}
	return nil
}

// func (c *CommandsKafka) WhoTopicPart(client sarama.Client, filePath string) (map[string][]int32, error) {

// 	brokerIDs, err := c.broker.brokerList(client)
// 	if err != nil {
// 		return nil, err
// 	}

// 	needReplicaBrokerId, replicaBrokerId, err := c.topic.whoTopicPart(client, filePath, brokerIDs)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return needReplicaBrokerId, replicaBrokerId, nil
// }
