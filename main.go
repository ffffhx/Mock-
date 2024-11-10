package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	reader *kafka.Reader
	topic  = "user_click"
)

// 生产消息
func writerKafka(ctx context.Context) {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		WriteTimeout:           1 * time.Second,
		RequiredAcks:           kafka.RequireNone,
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()
	for i := 0; i < 3; i++ {
		if err := writer.WriteMessages(
			ctx,
			kafka.Message{Key: []byte("1"), Value: []byte("万幸")},
			kafka.Message{Key: []byte("2"), Value: []byte("得以")},
			kafka.Message{Key: []byte("3"), Value: []byte("存活")},
		); err != nil {
			if err == kafka.LeaderNotAvailable {
				time.Sleep(10 * time.Second)
				continue
			} else {
				fmt.Println("批量写kafka失败:", err)
			}
		} else {
			fmt.Println("写入消息成功")
		}

	}
}

// 消费消息
func readKafka(ctx context.Context) {
	reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          topic,
		CommitInterval: 1 * time.Second,
		GroupID:        "rec_team",
		StartOffset:    kafka.FirstOffset, // earliest or latest
	})
	//defer reader.Close() //kill的时候这个defer不执行，reader不关闭，所以要监听kill信号（2和15）

	for {
		if message, err := reader.ReadMessage(ctx); err != nil {
			fmt.Println("读取消息失败:", err)
			break
		} else {
			fmt.Println("读取消息成功:", message.Value)
			fmt.Println("topic=%s, partition=%d, offset=%d, key=%s, value=%s", message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))
		}
	}
}

// 监听信息2和15，当收到信号的时候关闭reader
func listenSignal() {
	//注册信号
	c := make(chan os.Signal, 1)                      //创建信号管道，容量为1
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM) //监听信号2和15，当收到这两个信号的时候放到c管道里
	sig := <-c                                        //从管道里读出这个信号，如果没有收到信号，这行读管道的代码会堵塞
	fmt.Println("收到信号", sig.String())
	//关闭reader之前判断是否为空，因为如果没有执行到readKafka，reader就是nil（全局变量里的哪个空指针）
	if reader != nil {
		reader.Close() //关闭reader
	}
	os.Exit(0) //退出程序

}

// 查询消息
func handleMessages(w http.ResponseWriter, r *http.Request) {
	messages := []map[string]interface{}{}
	for {
		if message, err := reader.ReadMessage(context.Background()); err != nil {
			fmt.Println("读取消息失败:", err)
			break
		} else {
			fmt.Println("读取消息成功:", message.Value)
			messages = append(messages, map[string]interface{}{
				"topic":     message.Topic,
				"partition": message.Partition,
				"offset":    message.Offset,
				"key":       string(message.Key),
				"value":     string(message.Value),
			})
		}

		fmt.Fprintf(w, "%v", messages)
	}
	//返回json响应
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(messages)
}
func main() {
	//ctx := context.Background()
	//writerKafka(ctx)

	go func() {
		http.HandleFunc("/api/v1/messages", handleMessages)
		log.Println("Starting server at port 8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatal(err)
		}
	}()

	go listenSignal()

}
