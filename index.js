const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const mysql = require('mysql2/promise');
const axios = require('axios');

const REQUIRED_ENV = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME', 'SII_API_KEY'];
for (const key of REQUIRED_ENV) {
  if (!process.env[key]) throw new Error(`Missing env var: ${key}`);
}

const lambda = new LambdaClient({ region: process.env.VAR_AWS_REGION });

const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  connectionLimit: 5,
  waitForConnections: true,
});

exports.handler = async (event) => {
  console.log('Lambda triggered', { recordCount: event.Records.length });

  const sanitizeText = (text) => {
    if (!text) return '';
    return text
      .replace(/[^\w\sáéíóúÁÉÍÓÚñÑ.,-]/g, '')
      .replace(/\s+/g, ' ')
      .trim();
  };

  for (const record of event.Records) {
    const body = JSON.parse(record.body);
    const { documentId, billData } = body;
    console.log('Procesando record', { documentId });

    const document = await documentById(documentId);

    if (!document) {
      console.error(`Documento con id ${documentId} no encontrado.`);
      return { statusCode: 200, body: 'Documento no encontrado' };
    }

    let emailPayload = null;

    try {
      if (billData?.recipient?.address) {
        billData.recipient.address = sanitizeText(billData.recipient.address);
      }

      if (billData?.detail) {
        billData.detail = sanitizeText(billData.detail);
      }

      billData.paymentMethod = 'transferencia';
      billData.billType = 'afecta';

      const response = await axios.post(
        'https://api-sii.agify.cl/api/sii/emit-sale-bill',
        billData,
        {
          timeout: 40000,
          headers: {
            'x-api-key': process.env.SII_API_KEY,
            'Content-Type': 'application/json',
          },
        }
      );

      const { pdfUrl, billDetailsSii } = response.data;
      const billNumber = billDetailsSii?.billNumber;
      console.log('Respuesta SII', { status: response.status, billNumber, hasPdfUrl: !!pdfUrl });

      if (response.status === 200 && pdfUrl && billNumber) {
        const num = Number(billNumber);
        if (!Number.isInteger(num) || num <= 0) throw new Error(`billNumber inválido: ${billNumber}`);
        await updateDocumentStatus(documentId, 'issued', pdfUrl, num);
        emailPayload = { documentId, pdfUrl, billData };
      } else {
        await updateDocumentStatus(documentId, 'rejected', null, null);
        throw new Error(`Fallo en emisión. Datos incompletos o status ${response.status}`);
      }
    } catch (err) {
      console.error(`Error procesando documento ${documentId}:`, err.message);
      await updateDocumentStatus(documentId, 'rejected', null, null);
      throw err;
    }

    if (emailPayload) {
      await invokeSendEmailLambda(emailPayload);
    }
  }

  return {
    statusCode: 200,
    body: JSON.stringify({ message: 'Mensajes procesados correctamente' }),
  };
};

async function invokeSendEmailLambda(payload) {
  const functionName = 'notifyDocumentSii';

  const response = await lambda.send(new InvokeCommand({
    FunctionName: functionName,
    InvocationType: 'Event',
    Payload: Buffer.from(JSON.stringify(payload)),
  }));

  console.log('Correo Lambda invocada', { statusCode: response.StatusCode });
}

async function updateDocumentStatus(documentId, status, pdfUrl, billNumber) {
  const [result] = await pool.execute(
    `UPDATE document_header
     SET status = ?, pdfUrl = ?, number = ?, updatedAt = CURRENT_TIMESTAMP
     WHERE id = ?`,
    [status, pdfUrl, billNumber, documentId]
  );

  if (result.affectedRows === 0) {
    throw new Error(`No se encontró el documento con id ${documentId}`);
  }
}

async function documentById(documentId) {
  const [rows] = await pool.execute(
    `SELECT * FROM document_header WHERE id = ?`,
    [documentId]
  );
  return rows[0] ?? null;
}
/*
if (require.main === module) exports.handler({
    "Records": [
        {
            "messageId": "ba98098a-e0d2-4790-b1f5-0c2fad1c9fde",
            "receiptHandle": "AQEBOBbzKg0sKzyAt/tE5NQvE2mNe+Zu6k5Gw16u3wnFDgbH73jR2MDIbiq/7nqZghFV8/1o/bevX+alMKFTmzLNx6y58QZBxAXsBllGONV9wMIWaRI8VJKgTLeufrIJXwb7blbxsQ51tMw/QW1LmrO8xY19pGU+PJTUHkTqeydPHcbl1V2UwhgxUsVq+5L+XTLnUhykeOY8kFS/z1P1eQ35VHAkgUM+5dFa4kqTmEG/SHk9Xn/Is+y26WYaU7Fm+TsG68fwCYhWIQcy99xdnRYLsyq6o4AbI4JalHM5+CIhW19fY/NQcL6iwCj3ZlhnhhppFH9fZFwIP0vJBPXwvs1aoxwNoIgFB/v9Zqi2Kn3RVTy7hOO3h40f7oGUzVXM1n8vww5ligB6vxE6+B25H54qmQ==",
            "body":  "{\"documentId\":\"d627e5d6-a5a6-4342-bc87-b21007311760\",\"billData\":{\"amount\":200,\"recipient\":{\"rut\":\"16893186-7\",\"name\":\"Eugenio Julio\",\"address\":\"bustos\",\"email\":\"eugenio.julio@live.com\"},\"detail\":\"Consultoria Arquitectura\"}}",
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1779405606476",
                "SenderId": "AIDA3OCJ24L27YPXJ7UQF",
                "ApproximateFirstReceiveTimestamp": "1779405606480"
            },
            "messageAttributes": {},
            "md5OfBody": "9e6e506a9b1291d08d436611760cac5c",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-2:786133869301:documents-sii",
            "awsRegion": "us-east-2"
        }
    ]
}).then(console.log).catch(console.error);
*/