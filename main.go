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
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/xdg-go/scram"
)

// func init() {
// 	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags|log.Lshortfile)
// }

func main() {

	// Определяем флаги командной строки
	generateFlag := pflag.BoolP("mapPartGenerate", "g", false, "Выполнить генерацию файлов с топиками, для перераспределения партиций, если добавить -f, то будет использоваться путь к файлу с топиками")
	applyFlag := pflag.BoolP("mapPartApply", "a", false, "Применить перераспределение партиций")
	verifyFlag := pflag.BoolP("mapPartVerify", "v", false, "Проверить перераспределение партиций")
	rollbackFlag := pflag.BoolP("mapPartRollback", "r", false, "Откатить перераспределение партиций к предыдущему состоянию")
	helpFlag := pflag.BoolP("help", "h", false, "Вывести справку")
	topicsFile := pflag.StringP("file", "f", "", "Путь к файлу со списком существующих топиков")
	createTopicFile := pflag.StringP("createTopic", "", "", "Создать топик, используется ключ и путь до yaml файла: --createTopic /topics/test.yaml")
	createUserFile := pflag.StringP("createUser", "", "", "Создать пользователя, используется ключ и путь до yaml файла: --createUser /users/test.yaml")
	createUserAclFile := pflag.StringP("createUserAcl", "", "", "Добавить ACL для пользователя, используется ключ и путь до yaml файла: --createUserAcl /users/test.yaml")
	aclList := pflag.StringP("aclList", "", "", "Вывести список ACL для пользователя, используется ключ и имя пользователя: --aclList test")
	// whoTopicPart := pflag.StringP("whoTopicPart", "", "", "Вывести список партиций топиков, используется ключ и путь до yaml файла: --whoTopicPart /topics/test.yaml")

	// Парсим флаги
	pflag.Parse()

	if *helpFlag || pflag.NFlag() == 0 {
		pflag.PrintDefaults()
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
	if *rollbackFlag {
		if err := cmd.TopicRollbackReassignPart(); err != nil {
			log.Printf("Ошибка отката перераспределения партиций топиков: %v", err)
		}
		log.Printf("==========================================================================")
		log.Printf("✅ Задача по откату перераспределения партиций топиков, успешно выполнена!")
		log.Printf("==========================================================================")
	}

	if *generateFlag {
		if err := cmd.TopicGenerateReassignPart(client, *topicsFile); err != nil {
			log.Printf("============================================================================")
			log.Printf("❌ Задача генерации файлов с топиками для перераспределения партиций, не выполнена!")
			log.Printf("Ошибка: %v", err)
			log.Printf("============================================================================")
		} else {
			log.Printf("=========================================================================")
			log.Printf("✅ Файлы с топиками для перераспределения партиций успешно сгенерированы!")
			log.Printf("=========================================================================")
		}
	}

	if *applyFlag {
		if err := cmd.TopicApplyReassignPart(); err != nil {
			log.Printf("Ошибка применения перераспределение партиций топиков: %v", err)
		}
		log.Printf("===================================================================")
		log.Printf("✅ Задача по перераспределению партиций топиков, успешно запущена!")
		log.Printf("===================================================================")
	}

	if *verifyFlag {
		if err := cmd.TopicVerifyReassignPart(); err != nil {
			log.Printf("Ошибка проверки перераспределения партиций топиков: %v", err)
		}
		log.Printf("============================================================================")
		log.Printf("✅ Задача по проверке перераспределения партиций топиков, успешно выполнена!")
		log.Printf("============================================================================")
	}

	if *createTopicFile != "" {
		if err := cmd.TopicCreate(*createTopicFile); err != nil {
			log.Printf("============================================================================")
			log.Printf("❌ Задача по созданию топика, не выполнена!")
			log.Printf("============================================================================")
		} else {
			log.Printf("============================================================================")
			log.Printf("✅ Задача по созданию топика, успешно выполнена!")
			log.Printf("============================================================================")
		}
	}

	if *createUserFile != "" {
		if err := cmd.UserCreate(*createUserFile); err != nil {
			log.Printf("Ошибка при создании пользователя: %v", err)
		}
	}

	if *createUserAclFile != "" {
		if err := cmd.UserAddAcl(*createUserAclFile); err != nil {
			log.Printf("Ошибка при добавлении ACL для пользователя: %v", err)
		}
	}

	if *aclList != "" {
		if err := cmd.AclList(*aclList); err != nil {
			log.Printf("Ошибка при выводе ACL для пользователя: %v", err)

		}
	}

	// if *whoTopicPart != "" {
	// 	data, replicaBrokerId, err := cmd.WhoTopicPart(client, *whoTopicPart)
	// 	if err != nil {
	// 		log.Printf("Ошибка вывода списка партиций топиков: %v", err)
	// 	} else {
	// 		for topic, brokers := range data {
	// 			log.Printf("Топик: %s, Брокеры: %v", topic, brokers)
	// 		}
	// 		for topic, brokers := range replicaBrokerId {
	// 			log.Printf("Топик: %s, Брокеры: %v", topic, brokers)
	// 		}
	// 	}
	// }

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
