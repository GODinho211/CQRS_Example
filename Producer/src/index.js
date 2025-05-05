const express = require('express');
const { Client } = require('pg');

const app = express();
const PORT = 3000;


const { Kafka } = require('kafkajs');
const kafka = new Kafka({ brokers: ['localhost:9092'] });

const producer = kafka.producer();
producer.connect().then(() => {
  console.log('Producer Kafka conectado');
}).catch(err => {
  console.error('Erro ao conectar producer Kafka:', err);
});


// Conectar ao PostgreSQL
const client = new Client({
  user: 'postgres',
  host: 'localhost',
  database: 'exemplo_estagio',
  password: 'nova_senha',
  port: 5432,
});

client.connect()
  .then(() => {
    console.log('Conectado ao PostgreSQL');
  })
  .catch(err => {
    console.error('Erro ao conectar ao PostgreSQL', err);
  });

// Middleware para ler JSON
app.use(express.json());

// POST /users — Recebe dados e guarda na base de dados
app.post('/users', async (req, res) => {
  const { name, email, password } = req.body;

  if (!name || !email || !password) {
    return res.status(400).json({ erro: "Nome, email e senha são obrigatórios." });
  }

  try {
    const result = await client.query(
      'INSERT INTO users (name, email, password) VALUES ($1, $2, $3) RETURNING *',
      [name, email, password]
    );

    const newUser = result.rows[0];

    // Enviar para Kafka
    const eventPayload = {
      eventType: 'created',
      timestamp: new Date().toISOString(),
      user: {
        id: newUser.id,
        name: newUser.name,
        email: newUser.email
        // (evita enviar a password aqui!)
      }
    };

    await producer.send({
      topic: 'user',
      messages: [
        {
          key: newUser.id.toString(),
          value: JSON.stringify(eventPayload),
        },
      ],
    });

    res.status(201).json({
      mensagem: "Utilizador criado com sucesso!",
      dados: newUser
    });

  } catch (err) {
    console.error('Erro ao inserir utilizador:', err);
    res.status(500).json({ erro: "Erro ao criar o utilizador." });
  }
});

// Inicia o servidor
app.listen(PORT, () => {
  console.log(`Servidor a correr em http://localhost:${PORT}`);
});

