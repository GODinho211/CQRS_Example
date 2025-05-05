const { Kafka } = require('kafkajs');

// Configuração do cliente Kafka
const kafka = new Kafka({
  clientId: 'user-topic-creator',
  brokers: ['localhost:9092'], // ajuste para o endereço do seu broker Kafka
});

// Função para criar o tópico
const createUserTopic = async () => {
  const admin = kafka.admin();
  await admin.connect();

  const topicName = 'user';

  const topicConfig = {
    topic: topicName,
    numPartitions: 3,
    replicationFactor: 1, // ajuste para >= 2 se estiver usando múltiplos brokers
  };

  try {
    const topicsCreated = await admin.createTopics({
      topics: [topicConfig],
      waitForLeaders: true,
    });

    if (topicsCreated) {
      console.log(`✅ Tópico "${topicName}" criado com sucesso.`);
    } else {
      console.log(`ℹ️ Tópico "${topicName}" já existe.`);
    }
  } catch (error) {
    console.error(`❌ Erro ao criar o tópico:`, error);
  } finally {
    await admin.disconnect();
  }
};

createUserTopic();
