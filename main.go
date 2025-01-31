package main

import (
	"crypto/sha256"
	"crypto/sha512"
	"flag"
	"kafkamap/commands"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/spf13/viper"
	"github.com/xdg-go/scram"
)

// func init() {
// 	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags|log.Lshortfile)
// }

func main() {
	// Определяем флаги командной строки
	generateFlag := flag.Bool("g", false, "Выполнить генерацию файлов с топиками, для перераспределения партиций")
	applyFlag := flag.Bool("a", false, "Применить перераспределение партиций")
	verifyFlag := flag.Bool("v", false, "Проверить перераспределение партиций")
	helpFlag := flag.Bool("h", false, "Вывести справку")

	// Парсим флаги
	flag.Parse()

	if *helpFlag {
		flag.PrintDefaults()
		os.Exit(0)
	}

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

	// Выполняем команды в зависимости от флагов

	if *generateFlag {
		if err := cmd.TopicGenerateReassignPart(client); err != nil {
			log.Printf("Ошибка генерации топиков: %v", err)
		}
		log.Printf("Файлы с топиками успешно сгенерированы")
	}

	if *applyFlag {
		if err := cmd.TopicApply(); err != nil {
			log.Printf("Ошибка применения топиков: %v", err)
		}
	}

	if *verifyFlag {
		if err := cmd.TopicVerify(); err != nil {
			log.Printf("Ошибка проверки топиков: %v", err)
		}
	}

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
