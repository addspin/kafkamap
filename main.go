package main

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/spf13/viper"
	"github.com/xdg-go/scram"
)

func main() {

	viper.SetConfigFile("config.yaml")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}

	config := sarama.NewConfig()
	config.Version = sarama.V3_9_0_0

	// Настройка SASL PLAIN аутентификации
	config.Net.SASL.Enable = viper.GetBool("kafka.sasl.enabled")
	// if viper.GetString("kafka.sasl.mechanism") == "PLAIN" {
	config.Net.SASL.Mechanism = sarama.SASLMechanism(viper.GetString("kafka.sasl.mechanism"))
	// }

	// Настройка SCRAM аутентификации
	// if viper.GetString("kafka.sasl.mechanism") == "SCRAM-SHA-256" {
	// 	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
	// 		return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
	// 	}
	// 	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	// }
	// if viper.GetString("kafka.sasl.mechanism") == "SCRAM-SHA-512" {
	// 	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
	// 		return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
	// 	}
	// 	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	// } else {
	// 	log.Fatalf("Неверный алгоритм SHA \"%s\": должен быть \"SCRAM-SHA-256\" или \"SCRAM-SHA-512\"", viper.GetString("kafka.sasl.mechanism"))
	// }

	config.Net.SASL.User = viper.GetString("kafka.sasl.username")
	config.Net.SASL.Password = viper.GetString("kafka.sasl.password")

	// Указываем протокол безопасности
	config.Net.TLS.Enable = viper.GetBool("kafka.tls.enabled")
	config.Net.SASL.Handshake = viper.GetBool("kafka.sasl.handshake")

	// Таймауты для соединения с брокером клиентом
	config.Net.DialTimeout = viper.GetDuration("kafka.timeout.dial")
	config.Net.ReadTimeout = viper.GetDuration("kafka.timeout.read")
	config.Net.WriteTimeout = viper.GetDuration("kafka.timeout.write")

	// Адреса брокеров из вашего кластера
	brokers := viper.GetStringSlice("kafka.broker")

	var client sarama.Client
	var err error

	// Функция для создания клиента
	createClient := func() error {
		client, err = sarama.NewClient(brokers, config)
		if err != nil {
			log.Printf("Error creating client: %v", err)
			return err
		}
		log.Println("Kafka client created successfully")
		return nil
	}

	// Первичное создание клиента
	if err := createClient(); err != nil {
		log.Fatalf("Initial client creation failed: %v", err)
	}

	if err := client.RefreshMetadata(); err != nil {
		log.Printf("Connection lost, attempting to reconnect: %v", err)
		if err := createClient(); err != nil {
			log.Printf("Reconnection failed: %v", err)
		}
	}
	topicList(client)

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

func topicList(client sarama.Client) {
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		log.Printf("Error creating admin client: %v", err)
		return
	}
	defer admin.Close()

	// Получаем список топиков
	topics, err := admin.ListTopics()
	if err != nil {
		log.Printf("Error listing topics: %v", err)
		return
	}

	// Сбор имен топиков
	var topicList []map[string]string
	for topicName := range topics {
		if topicName == "__consumer_offsets" {
			continue
		}
		topicList = append(topicList, map[string]string{"topic": topicName})
	}

	// Создание JSON структуры
	data := map[string]interface{}{
		"topics":  topicList,
		"version": 1,
	}

	// Запись в JSON файл
	file, err := os.Create("topics.json")
	if err != nil {
		log.Printf("Error creating JSON file: %v", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		log.Printf("Error encoding JSON: %v", err)
		return
	}

	log.Println("JSON file created successfully")
}

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
