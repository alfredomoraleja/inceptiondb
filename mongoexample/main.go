package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	// 1️⃣ Crear contexto con timeout para evitar bloqueos
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 2️⃣ Conectarse a MongoDB (puede ser local o remoto)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal("❌ Error conectando a MongoDB:", err)
	}

	// 3️⃣ Comprobar conexión
	if err := client.Ping(ctx, nil); err != nil {
		log.Fatal("❌ No se pudo hacer ping a MongoDB:", err)
	}
	fmt.Println("✅ Conectado a MongoDB")

	// 4️⃣ Seleccionar base de datos y colección
	collection := client.Database("testdb").Collection("users")

	// 5️⃣ Insertar un documento
	doc := bson.M{"name": "Gerardo", "age": 35}
	insertResult, err := collection.InsertOne(ctx, doc)
	if err != nil {
		log.Fatal("❌ Error al insertar documento:", err)
	}
	fmt.Println("✅ Documento insertado con _id:", insertResult.InsertedID)

	// 6️⃣ Buscar un documento
	var result bson.M
	filter := bson.M{"name": "Gerardo"}
	err = collection.FindOne(ctx, filter).Decode(&result)
	if err == mongo.ErrNoDocuments {
		fmt.Println("⚠️ No se encontró ningún documento con ese filtro")
	} else if err != nil {
		log.Fatal("❌ Error al buscar documento:", err)
	} else {
		fmt.Println("✅ Documento encontrado:", result)
	}

	// 7️⃣ Cerrar conexión
	if err := client.Disconnect(ctx); err != nil {
		log.Fatal("❌ Error al cerrar conexión:", err)
	}
	fmt.Println("🔌 Conexión cerrada")
}
