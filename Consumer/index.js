const { Kafka } = require('kafkajs');
const { Pool } = require('pg');

// Conectar à base de dados PostgreSQL
async function connectDb() {
  const pool = new Pool({
    user: 'postgres', // Seu usuário do PostgreSQL
    host: 'localhost', // O host do banco de dados
    database: 'userquery', // Nome da base de dados
    password: 'nova_senha', // Sua senha do PostgreSQL
    port: 5432, // Porta padrão do PostgreSQL
  });

  try {
    const client = await pool.connect();
    console.log('Conexão com a base de dados bem-sucedida!');
    return client;
  } catch (err) {
    console.error('Erro ao conectar à base de dados:', err);
    process.exit(1);
  }
}

// Configurar o Kafka
const kafka = new Kafka({
  clientId: 'my-consumer',
  brokers: ['localhost:9092'], // Substitua pelo endereço correto do seu broker Kafka
});

// Criar o consumidor Kafka
const consumer = kafka.consumer({ groupId: 'user-group' });

async function run() {
  // Conectar ao Kafka
  await consumer.connect();
  console.log('Conectado ao Kafka');

  // Inscrever-se nas 3 partições do tópico 'user'
  await consumer.subscribe({ topic: 'user', fromBeginning: true });
  console.log('Inscrito no tópico user');

  // Consumir mensagens
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const { eventType, user } = JSON.parse(message.value.toString()); // Desestruturação corrigida

      console.log(`Mensagem recebida na partição ${partition}:`, user);

      // Verificar o tipo de evento
      if (eventType === 'created') {
        // Conectar ao banco de dados e armazenar os dados do usuário
        const dbClient = await connectDb();
        try {
          // Inserir novo usuário na tabela
          await dbClient.query(
            'INSERT INTO public.users(id, name, email) VALUES($1, $2, $3)',
            [user.id, user.name, user.email]
          );
          console.log(`Usuário ${user.id} criado com sucesso!`);
        } catch (err) {
          console.error('Erro ao processar a mensagem:', err);
        } finally {
          // Liberar o cliente de conexão do banco de dados
          dbClient.release();
        }
      } else {
        console.log('Tipo de operação não suportado para o momento:', eventType);
      }
    },
  });
}

run().catch(console.error);
