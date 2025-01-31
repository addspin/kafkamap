package main

import (
	"crypto/sha256"
	"crypto/sha512"
	"kafkamap/commands"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/spf13/viper"
	"github.com/xdg-go/scram"
)

func init() {
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags|log.Lshortfile)
}

func main() {

	viper.SetConfigFile("config.yaml")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}

	config := sarama.NewConfig()
	config.Version = sarama.V3_9_0_0
	config.ClientID = "kafkamap-client"

	// Настройка SASL PLAIN аутентификации
	config.Net.SASL.Enable = viper.GetBool("kafka.sasl.enabled")
	config.Net.SASL.User = viper.GetString("kafka.sasl.username")
	config.Net.SASL.Password = viper.GetString("kafka.sasl.password")
	// config.Net.SASL.Mechanism = sarama.SASLMechanism(viper.GetString("kafka.sasl.mechanism"))

	// Указываем протокол безопасности
	config.Net.TLS.Enable = viper.GetBool("kafka.tls.enabled")
	config.Net.SASL.Handshake = viper.GetBool("kafka.sasl.handshake")

	// Таймауты для соединения с брокером клиентом
	config.Net.DialTimeout = viper.GetDuration("kafka.timeout.dial")
	config.Net.ReadTimeout = viper.GetDuration("kafka.timeout.read")
	config.Net.WriteTimeout = viper.GetDuration("kafka.timeout.write")

	// Адреса брокеров из вашего кластера
	brokers := viper.GetStringSlice("kafka.broker")

	// Настройка SASL аутентификации
	log.Printf("Настройка SASL механизма: %s", viper.GetString("kafka.sasl.mechanism"))
	switch viper.GetString("kafka.sasl.mechanism") {
	case "PLAIN":
		log.Printf("Настройка PLAIN аутентификации")
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		config.Net.SASL.SCRAMClientGeneratorFunc = nil // Очищаем SCRAM конфигурацию для PLAIN
		log.Printf("SASL конфигурация: Enable=%v, User=%s, Handshake=%v",
			config.Net.SASL.Enable,
			config.Net.SASL.User,
			config.Net.SASL.Handshake)
	case "SCRAM-SHA-256":
		log.Printf("Настройка SCRAM-SHA-256 аутентификации")
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
		}
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	case "SCRAM-SHA-512":
		log.Printf("Настройка SCRAM-SHA-512 аутентификации")
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
		}
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	default:
		log.Fatalf("Неподдерживаемый механизм SASL \"%s\": должен быть \"PLAIN\", \"SCRAM-SHA-256\" или \"SCRAM-SHA-512\"",
			viper.GetString("kafka.sasl.mechanism"))
	}

	var client sarama.Client
	var err error

	// Функция для создания клиента
	createClient := func() error {
		log.Printf("Попытка подключения к брокерам: %v", brokers)
		log.Printf("Текущая конфигурация: SASL=%v, TLS=%v, Mechanism=%v",
			config.Net.SASL.Enable,
			config.Net.TLS.Enable,
			config.Net.SASL.Mechanism)

		client, err = sarama.NewClient(brokers, config)
		if err != nil {
			log.Printf("Ошибка создания клиента (детально): %+v", err)
			return err
		}
		log.Println("Kafka клиент успешно создан")
		return nil
	}

	// Первичное создание клиента
	if err := createClient(); err != nil {
		log.Printf("Критическая ошибка при создании клиента: %+v", err)
		log.Fatalf("Initial client creation failed: %v", err)
	}

	if err := client.RefreshMetadata(); err != nil {
		log.Printf("Connection lost, attempting to reconnect: %v", err)
		if err := createClient(); err != nil {
			log.Printf("Reconnection failed: %v", err)
		}
	}

	cmd := commands.NewCommandKafka()
	cmd.TopicVerify(client)

	// Обработка сигналов для корректного закрытия
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-c
		log.Printf("Received signal %v, shutting down...", sig)
		if err := client.Close(); err != nil {
			log.Printf("Error closing client: %v", err)
		}
		os.Exit(0)
	}()
}

// func topicList(client sarama.Client) error {
// 	admin, err := sarama.NewClusterAdminFromClient(client)
// 	if err != nil {
// 		log.Printf("Error creating admin client: %v", err)
// 		return err
// 	}
// 	defer admin.Close()

// 	// Получаем список топиков
// 	topics, err := admin.ListTopics()
// 	if err != nil {
// 		log.Printf("Error listing topics: %v", err)
// 		return err
// 	}

// 	// Сбор имен топиков
// 	var topicList []map[string]string
// 	for topicName := range topics {
// 		if topicName == "__consumer_offsets" {
// 			continue
// 		}
// 		topicList = append(topicList, map[string]string{"topic": topicName})
// 	}

// 	// Создание JSON структуры
// 	data := map[string]interface{}{
// 		"topics":  topicList,
// 		"version": 1,
// 	}

// 	// Запись в JSON файл
// 	file, err := os.Create("topics.json")
// 	if err != nil {
// 		log.Printf("Error creating JSON file: %v", err)
// 		return err
// 	}
// 	defer file.Close()

// 	encoder := json.NewEncoder(file)
// 	encoder.SetIndent("", "  ")
// 	if err := encoder.Encode(data); err != nil {
// 		log.Printf("Error encoding JSON: %v", err)
// 		return err
// 	}

// 	log.Println("JSON file created successfully")
// 	return nil
// }

// func ExecuteKafkaCommand() error {
// 	config := fmt.Sprintf(`security.protocol=%s
// sasl.mechanism=%s
// sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";`, viper.GetString("kafka.securityProtocol"), viper.GetString("kafka.sasl.mechanism"), viper.GetString("kafka.sasl.username"), viper.GetString("kafka.sasl.password"))

// 	cmd := exec.Command("docker", "exec", "kafka", "bash", "-c",
// 		fmt.Sprintf(`echo '%s' > /tmp/config.properties && kafka-topics.sh --describe --bootstrap-server localhost:9092 --command-config /tmp/config.properties && rm /tmp/config.properties`, config))

// 	cmd.Stdout = os.Stdout
// 	cmd.Stderr = os.Stderr

// 	return cmd.Run()
// }

var (
	SHA256 scram.HashGeneratorFcn = sha256.New
	SHA512 scram.HashGeneratorFcn = sha512.New
)

type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
