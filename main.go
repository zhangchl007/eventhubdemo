package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/checkpoints"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

type Config struct {
	ContainerName      string `yaml:"container_name"`
	StorageAccountName string `yaml:"storage_account_name"`
	EventHubNamespace  string `yaml:"event_hub_namespace"`
	EventHubName       string `yaml:"event_hub_name"`
	TenantID           string `yaml:"tenant_id"`
	ClientID           string `yaml:"client_id"`
	ClientSecret       string `yaml:"client_secret"`
}

func main() {
	config, err := loadConfig("config.yaml")
	if err != nil {
		panic(err)
	}

	fmt.Printf("ContainerName: %s\n", config.ContainerName)
	fmt.Printf("StorageAccountName: %s\n", config.StorageAccountName)
	fmt.Printf("EventHubNamespace: %s\n", config.EventHubNamespace)
	fmt.Printf("EventHubName: %s\n", config.EventHubName)
	fmt.Printf("TenantID: %s\n", config.TenantID)
	fmt.Printf("ClientID: %s\n", config.ClientID)

	if config.ContainerName == "" || config.StorageAccountName == "" || config.EventHubNamespace == "" || config.EventHubName == "" || config.TenantID == "" || config.ClientID == "" || config.ClientSecret == "" {
		panic("Configuration file must contain container_name, storage_account_name, event_hub_name, event_hub_namespace, tenant_id, client_id, and client_secret")
	}

	// create a service principal credential
	cred, err := azidentity.NewClientSecretCredential(config.TenantID, config.ClientID, config.ClientSecret, nil)
	if err != nil {
		panic(err)
	}

	// create a container client using service principal
	checkClient, err := container.NewClient(fmt.Sprintf("https://%s.blob.core.windows.net/%s", config.StorageAccountName, config.ContainerName), cred, nil)
	if err != nil {
		panic(err)
	}

	// create a checkpoint store that will be used by the event hub
	checkpointStore, err := checkpoints.NewBlobStore(checkClient, nil)
	if err != nil {
		panic(err)
	}

	// create a consumer client using service principal
	consumerClient, err := azeventhubs.NewConsumerClient(config.EventHubNamespace, config.EventHubName, azeventhubs.DefaultConsumerGroup, cred, nil)
	if err != nil {
		panic(err)
	}

	defer consumerClient.Close(context.TODO())

	// create a processor to receive and process events
	processor, err := azeventhubs.NewProcessor(consumerClient, checkpointStore, nil)
	if err != nil {
		panic(err)
	}

	// for each partition in the event hub, create a partition client with processEvents as the function to process events
	dispatchPartitionClients := func() {
		for {
			partitionClient := processor.NextPartitionClient(context.TODO())
			if partitionClient == nil {
				break
			}

			go func() {
				if err := processEvents(partitionClient); err != nil {
					panic(err)
				}
			}()
		}
	}

	// run all partition clients
	go dispatchPartitionClients()

	processorCtx, processorCancel := context.WithCancel(context.TODO())
	defer processorCancel()

	if err := processor.Run(processorCtx); err != nil {
		panic(err)
	}
}

func loadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func processEvents(partitionClient *azeventhubs.ProcessorPartitionClient) error {
	defer closePartitionResources(partitionClient)
	for {
		receiveCtx, receiveCtxCancel := context.WithTimeout(context.TODO(), time.Minute)
		events, err := partitionClient.ReceiveEvents(receiveCtx, 100, nil)
		receiveCtxCancel()

		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		fmt.Printf("Processing %d event(s)\n", len(events))
/*
		for _, event := range events {
			eventData, err := json.Marshal(convertEvent(event))
			if err != nil {
				return err
			}
			fmt.Printf("Event received: %s\n", eventData)
		}*/
		for _, event := range events {
			var parsedJSON map[string]interface{}
			if err := json.Unmarshal(event.Body, &parsedJSON); err != nil {
				return err
			}
			parsedJSONBytes, err := json.MarshalIndent(parsedJSON, "", "    ")
			if err != nil {
				return err
			}
			fmt.Println(string(parsedJSONBytes))

		}

		if len(events) != 0 {
			if err := partitionClient.UpdateCheckpoint(context.TODO(), events[len(events)-1], nil); err != nil {
				return err
			}
		}
	}
}

func convertEvent(event *azeventhubs.ReceivedEventData) map[string]interface{} {
	result := make(map[string]interface{})
	result["Body"] = string(event.Body)
	result["Properties"] = event.Properties
	result["SystemProperties"] = event.SystemProperties
	return result
}

func closePartitionResources(partitionClient *azeventhubs.ProcessorPartitionClient) {
	defer partitionClient.Close(context.TODO())
}

