# go-nats-wrapper

Переиспользуемые хелперы для NATS Jetstream

## Основные сценарии использования

Обертка предназначена для сервисов, которые используют NATS JetStream как
надежную очередь фоновых задач. Типичный сценарий: приложение-клиент публикует
задачу в stream, а consumer читает ее из очереди и передает дальше - во внешний
сервис, обработчик бизнес-логики или другой асинхронный процесс.

### Provisioning очередей

`Provisioner` создает или обновляет JetStream-ресурсы из конфигурации сервиса:

* work queue stream с заданными subjects;
* durable pull consumer для воркеров;
* отдельный DLQ stream, если для очереди нужна dead letter queue.

Это позволяет держать описание очередей в конфиге приложения и поднимать NATS
ресурсы отдельной командой перед запуском API, producers и воркеров.

### Публикация задач в stream

`StreamPublisher` публикует payload в subject и возвращает ack JetStream:
stream, sequence и признак duplicate. Сценарий подходит для API или
диспетчеров, которым нужно быстро зафиксировать задачу в очереди и не выполнять
долгую работу синхронно с запросом.

Publisher можно использовать, чтобы:

* положить входящую команду или событие в subject нужной очереди;
* разложить одну входящую задачу на несколько внутренних задач для разных
  обработчиков;
* передать работу из синхронного процесса в фоновые worker-процессы.

### Pull-обработка задач воркерами

`PullConsumer` скрывает получение сообщений из durable consumer и дает воркеру
три базовые операции:

* `NextMessage` - забрать следующую задачу;
* `Ack` - подтвердить успешную обработку;
* `Nack` - вернуть задачу в очередь с задержкой `NakDelay`.

Такой сценарий рассчитан на независимые worker-процессы, которые можно
масштабировать горизонтально. Несколько экземпляров читают один durable
consumer, а JetStream распределяет задачи между ними.

### Retry и DLQ

При `Nack` обертка смотрит metadata сообщения. Пока число доставок меньше
`MaxDeliver`, сообщение возвращается в очередь через `NakWithDelay`. Когда
лимит доставок достигнут, сообщение:

* публикуется в `DLQSubject`, если он настроен;
* подтверждается через `Ack`, чтобы оно не зацикливалось в основной очереди.

Если DLQ не настроен, сообщение после достижения `MaxDeliver` просто
подтверждается. Если публикация в DLQ завершилась ошибкой, исходное сообщение не
ack-ается, чтобы потеря задачи не была скрыта.

### Управление соединением

Все компоненты используют общую настройку JetStream-соединения: имя клиента,
таймаут подключения, reconnect-политику, размер reconnect buffer, user/password
и TLS-файлы. Для долгоживущих воркеров можно передать `OnClosed`: callback
вызывается после закрытия установленного соединения, если reconnect не помог.
В приложении это удобно использовать как fatal-сигнал для supervisor или
error-observer.

## Примеры использования

```go
connection := natswrapper.ConnectionConfig{
	Host:                "queue",
	Port:                4222,
	Name:                "service-name",
	ConnectTimeout:      3 * time.Second,
	ReconnectAttempts:   3,
	ReconnectInterval:   time.Second,
	ReconnectBufferSize: 8 << 20,
	OnClosed: func(err error) {
		// Trigger application shutdown or error reporting.
	},
}

jetStream := natswrapper.JetStreamConfig{
	Connection:       connection,
	OperationTimeout: 15 * time.Second,
}

provisioner, err := natswrapper.NewProvisioner(natswrapper.ProvisionerConfig{
	JetStream: jetStream,
	Streams: []natswrapper.StreamProvisionConfig{
		{
			Stream: natswrapper.StreamConfig{
				Name:     "SERVICE_NAME_TASKS",
				Subjects: []string{"service-name.tasks"},
			},
			DLQ: &natswrapper.StreamConfig{
				Name:     "SERVICE_NAME_TASKS_DLQ",
				Subjects: []string{"service-name.tasks.dlq"},
			},
			Consumers: []natswrapper.ConsumerProvisionConfig{
				{
					Durable:    "SERVICE_NAME_TASKS",
					MaxDeliver: 3,
					AckWait:    30 * time.Second,
				},
			},
		},
	},
}, logger)
if err != nil {
	return err
}
defer provisioner.Close()

if err := provisioner.Provision(ctx); err != nil {
	return err
}

publisher, err := natswrapper.NewStreamPublisher(natswrapper.StreamPublisherConfig{
	JetStream: jetStream,
}, logger)
if err != nil {
	return err
}
defer publisher.Close()

_, err = publisher.Publish(ctx, "service-name.tasks", payload)

consumer, err := natswrapper.NewPullConsumer(natswrapper.PullConsumerConfig{
	JetStream:  jetStream,
	Stream:     "SERVICE_NAME_TASKS",
	Consumer:   "SERVICE_NAME_TASKS",
	DLQSubject: "service-name.tasks.dlq",
	NakDelay:   30 * time.Second,
	MaxDeliver: 3,
}, logger)
```
