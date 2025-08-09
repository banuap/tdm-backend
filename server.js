const express = require('express');
const { VertexAI } = require('@google-cloud/vertexai');
const { GoogleAuth } = require('google-auth-library');
const cors = require('cors');
const ss = require('simple-statistics');
const { Pool } = require('pg');
const axios = require('axios'); // Add axios

const app = express(); // FIX: Initialize the app here, before it's used.

// --- Port.io Configuration ---
const PORT_API_URL = 'https://api.getport.io/v1';
const PORT_CLIENT_ID = process.env.PORT_CLIENT_ID || 'cZpH62o3H1Lw4je3DU3fFFrPYtOk2oxC'; // Replace with your ID
const PORT_CLIENT_SECRET = process.env.PORT_CLIENT_SECRET || 'tF6meoT7coJ8GbjrJLgqUxWbajdRAJBLMWEvcDjCBQPcQkjSyiEh5C9Z153Zsv0N'; // Replace with your secret

let portToken = null;

// Function to get a Port API token
const getPortToken = async () => {
  if (portToken) return portToken; // Use cached token
  const response = await axios.post(`${PORT_API_URL}/auth/access_token`, {
    clientId: PORT_CLIENT_ID,
    clientSecret: PORT_CLIENT_SECRET,
  });
  portToken = response.data.accessToken;
  return portToken;
};

// --- NEW PORT.IO ENDPOINT ---
app.get('/api/port/environments', async (req, res) => {
  try {
    const token = await getPortToken();
    const response = await axios.get(`${PORT_API_URL}/blueprints/testEnvironment/entities`, {
      headers: { Authorization: `Bearer ${token}` },
      params: { include: 'properties,relations' } // Get all data
    });
    res.json({ success: true, environments: response.data.entities });
  } catch (error) {
    console.error('Failed to fetch environments from Port:', error.response?.data || error.message);
    res.status(500).json({ success: false, error: 'Could not fetch environments from Port.io' });
  }
});


// const app = express(); // REMOVE THIS LINE
const port = process.env.PORT || 8100;

app.use(cors());
app.use(express.json());

const PROJECT_ID = 'sparkle-labs-310106';
const LOCATION = 'us-central1';

// Initialize Vertex AI with Gemini
const vertexAI = new VertexAI({
  project: PROJECT_ID,
  location: LOCATION,
  keyFilename: '../tdm-dashboard/credentials/sparkle-labs-310106-2c976dd2a94d.json'
});

// Initialize Google Auth for connection testing
const auth = new GoogleAuth({
  keyFilename: '../tdm-dashboard/credentials/sparkle-labs-310106-2c976dd2a94d.json',
  scopes: ['https://www.googleapis.com/auth/cloud-platform']
});

// 2. Add PostgreSQL connection details (use environment variables in production)
const pgConfig = {
  user: 'schwab_user', // or your user
  host: 'localhost',
  database: 'SchwabAdvisoryDB', // Default db to connect to for listing others
  password: 'SchwabStrong@Pass123', // your password
  port: 5434,
};

// --- NEW DATABASE ENDPOINTS ---

// Endpoint to list available PostgreSQL databases
app.get('/api/db/list', async (req, res) => {
  const pool = new Pool(pgConfig);
  try {
    const result = await pool.query("SELECT datname FROM pg_database WHERE datistemplate = false AND datname <> 'postgres';");
    const dbNames = result.rows.map(row => row.datname);
    res.json({ success: true, databases: dbNames });
  } catch (error) {
    console.error('Failed to list databases:', error);
    res.status(500).json({ success: false, error: error.message });
  } finally {
    await pool.end();
  }
});

// Endpoint to generate DDL and save data to a table
app.post('/api/db/save', async (req, res) => {
  const { data, database, tableName } = req.body;

  if (!data || data.length === 0 || !database || !tableName) {
    return res.status(400).json({ success: false, error: 'Missing data, database, or tableName.' });
  }

  try {
    // Step 1: Use LLM to generate the DDL (CREATE TABLE statement)
    const generativeModel = vertexAI.getGenerativeModel({ model: 'gemini-1.5-flash' });
    const sampleRecord = JSON.stringify(data[0], null, 2);
    const ddlPrompt = `
      Based on the following JSON object structure, generate a robust PostgreSQL CREATE TABLE statement.
      The table name must be "${tableName}".
      - Infer appropriate PostgreSQL data types (e.g., VARCHAR(255), INTEGER, NUMERIC, TIMESTAMP, BOOLEAN).
      - Add a 'id' column as a BIGSERIAL PRIMARY KEY.
      - Do not include any comments or explanations, only the SQL statement.

      JSON Sample:
      ${sampleRecord}
    `;
    
    const ddlResult = await generativeModel.generateContent(ddlPrompt);
    const ddl = ddlResult.response.candidates[0].content.parts[0].text.replace(/```sql|```/g, '').trim();

    // Step 2: Connect to the specified database and execute the DDL
    const dbPool = new Pool({ ...pgConfig, database });
    await dbPool.query(`DROP TABLE IF EXISTS "${tableName}";`); // Drop if exists for idempotency
    await dbPool.query(ddl);

    // Step 3: Prepare and execute INSERT statements
    const headers = Object.keys(data[0]);
    const placeholders = headers.map((_, i) => `$${i + 1}`).join(', ');
    const insertQuery = `INSERT INTO "${tableName}" ("${headers.join('", "')}") VALUES (${placeholders})`;

    // Use a transaction for bulk insert
    const client = await dbPool.connect();
    try {
      await client.query('BEGIN');
      for (const row of data) {
        const values = headers.map(header => row[header]);
        await client.query(insertQuery, values);
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e; // Re-throw to be caught by outer catch block
    } finally {
      client.release();
    }
    
    await dbPool.end();

    res.json({ success: true, message: `Successfully created table "${tableName}" in database "${database}" and inserted ${data.length} records.`, ddl });

  } catch (error) {
    console.error('Failed to save data to DB:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Endpoint to list tables in a database
app.get('/api/db/tables', async (req, res) => {
  const { database } = req.query;
  if (!database) {
    return res.status(400).json({ success: false, error: 'Database query parameter is required.' });
  }
  const pool = new Pool({ ...pgConfig, database });
  try {
    const result = await pool.query("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';");
    const tableNames = result.rows.map(row => row.tablename);
    res.json({ success: true, tables: tableNames });
  } catch (error) {
    console.error(`Failed to list tables for ${database}:`, error);
    res.status(500).json({ success: false, error: error.message });
  } finally {
    await pool.end();
  }
});

// Endpoint to generate API stubs for a table
app.post('/api/stubs/generate', async (req, res) => {
  const { database, tableName } = req.body;
  if (!database || !tableName) {
    return res.status(400).json({ success: false, error: 'Database and tableName are required.' });
  }

  const pool = new Pool({ ...pgConfig, database });
  try {
    // Get table schema
    const schemaResult = await pool.query(`
      SELECT column_name, data_type 
      FROM information_schema.columns 
      WHERE table_name = $1;
    `, [tableName]);
    const schema = schemaResult.rows;

    // Use LLM to generate stubs
    const generativeModel = vertexAI.getGenerativeModel({ model: 'gemini-1.5-flash' });
    const prompt = `
      Based on the following PostgreSQL table schema, generate a complete set of 5 realistic WireMock stubs for a standard REST API.
      The table is named "${tableName}".
      The schema is: ${JSON.stringify(schema, null, 2)}

      Generate stubs for the following 5 operations:
      1.  GET /api/${tableName}: Retrieve a list of items. The response body should be an array of objects matching the schema.
      2.  GET /api/${tableName}/{id}: Retrieve a single item by its ID. Use WireMock's request templating to capture the ID from the URL path and use it in the response.
      3.  POST /api/${tableName}: Create a new item. The request body should match the schema. The response should reflect the created item.
      4.  PUT /api/${tableName}/{id}: Update an existing item.
      5.  DELETE /api/${tableName}/{id}: Delete an item. The response should be a 204 No Content.

      For each stub, provide a unique "name".
      Return ONLY a single, valid JSON array containing the 5 generated stub objects. Do not include any text, explanations, or markdown formatting.
    `;

    const result = await generativeModel.generateContent(prompt);
    const stubsJson = result.response.candidates[0].content.parts[0].text.replace(/```json|```/g, '').trim();
    const stubs = JSON.parse(stubsJson);

    res.json({ success: true, stubs });

  } catch (error) {
    console.error('Failed to generate stubs:', error);
    res.status(500).json({ success: false, error: error.message });
  } finally {
    await pool.end();
  }
});

// Test connection endpoint
app.get('/api/vertex-ai/test', async (req, res) => {
  try {
    const authClient = await auth.getClient();
    const accessToken = await authClient.getAccessToken();
    
    res.json({
      success: true,
      projectId: PROJECT_ID,
      location: LOCATION,
      model: 'gemini-2.5-flash', // CHANGED: Updated model name
      authenticated: !!accessToken.token
    });
  } catch (error) {
    console.error('Connection test failed:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// A new helper function to infer schema from sample data (CSV or JSON)
function inferSchemaFromSample(sample) {
  sample = sample.trim();
  let fields = [];
  let name = 'Inferred Schema';

  try {
    // Try parsing as JSON array
    if (sample.startsWith('[')) {
      const data = JSON.parse(sample);
      if (Array.isArray(data) && data.length > 0 && typeof data[0] === 'object') {
        name = 'Inferred JSON Schema';
        const firstRecord = data[0];
        fields = Object.keys(firstRecord).map(key => ({
          name: key,
          type: typeof firstRecord[key] // Simple type inference
        }));
        return { name, fields };
      }
    }
  } catch (e) { /* Not valid JSON, will proceed to check for CSV */ }

  // Fallback to CSV parsing
  const lines = sample.split('\n').filter(line => line.trim() !== '');
  if (lines.length > 0) {
    name = 'Inferred CSV Schema';
    const headers = lines[0].split(',').map(h => h.trim());
    fields = headers.map(header => ({
      name: header,
      type: 'string' // Default to string for CSV, as it's safest
    }));
  }
  
  if (fields.length === 0) {
    throw new Error('Could not infer schema from the provided sample data. Please provide valid JSON or CSV.');
  }

  return { name, fields };
}

/**
 * Parses raw data (CSV or JSON string) into an array of objects, converting numeric values.
 * @param {string} dataString The raw data string.
 * @returns {Array<object>} Parsed data.
 */
function parseDataForMetrics(dataString) {
  dataString = dataString.trim();
  if (dataString.startsWith('[')) {
    try {
      return JSON.parse(dataString);
    } catch (e) { /* Fall through */ }
  }
  const lines = dataString.split('\n').filter(line => line.trim() !== '');
  if (lines.length < 2) return [];
  const headers = lines[0].split(',').map(h => h.trim());
  return lines.slice(1).map(line => {
    const row = {};
    const values = line.split(',');
    headers.forEach((header, index) => {
      const value = values[index] ? values[index].trim() : '';
      row[header] = !isNaN(parseFloat(value)) && isFinite(value) ? parseFloat(value) : value;
    });
    return row;
  });
}

/**
 * Calculates ACTUAL data quality metrics by comparing original and generated datasets.
 * @param {string} originalSample The user-provided sample data (CSV or JSON).
 * @param {Array} generatedData The data generated by the AI.
 * @returns {object} An object containing the calculated quality scores.
 */
function calculateActualDataQualityMetrics(originalSample, generatedData) {
  const originalData = parseDataForMetrics(originalSample);
  if (originalData.length === 0 || generatedData.length === 0) {
    return { varianceMatch: 0, distributionSimilarity: 0, edgeCaseCoverage: 0 };
  }

  const originalHeaders = new Set(Object.keys(originalData[0]));
  const generatedHeaders = new Set(Object.keys(generatedData[0]));
  const commonHeaders = [...originalHeaders].filter(h => generatedHeaders.has(h));
  const numericHeaders = commonHeaders.filter(h => typeof originalData[0][h] === 'number');

  // Metric 1: Data Variance Match (Structural Similarity)
  const intersection = new Set([...originalHeaders].filter(h => generatedHeaders.has(h))).size;
  const union = new Set([...originalHeaders, ...generatedHeaders]).size;
  const varianceMatch = Math.round((intersection / union) * 100);

  let distributionScores = [];
  let edgeCaseScores = [];

  if (numericHeaders.length > 0) {
    numericHeaders.forEach(header => {
      const originalValues = originalData.map(r => r[header]).filter(v => typeof v === 'number');
      const generatedValues = generatedData.map(r => r[header]).filter(v => typeof v === 'number');
      if (originalValues.length < 2 || generatedValues.length < 2) return;

      // Metric 2: Distribution Similarity (Mean & StdDev)
      const originalMean = ss.mean(originalValues);
      const generatedMean = ss.mean(generatedValues);
      const originalStdDev = ss.standardDeviation(originalValues);
      const generatedStdDev = ss.standardDeviation(generatedValues);
      
      const meanSimilarity = 100 - (Math.abs(originalMean - generatedMean) / (Math.abs(originalMean) || 1)) * 100;
      const stdDevSimilarity = 100 - (Math.abs(originalStdDev - generatedStdDev) / (Math.abs(originalStdDev) || 1)) * 100;
      distributionScores.push((meanSimilarity + stdDevSimilarity) / 2);

      // Metric 3: Edge Case Coverage (Outlier Analysis)
      const outlierThreshold = originalMean + 2 * originalStdDev;
      const originalOutlierRate = originalValues.filter(v => v > outlierThreshold).length / originalValues.length;
      const generatedOutlierRate = generatedValues.filter(v => v > outlierThreshold).length / generatedValues.length;
      edgeCaseScores.push(100 - (Math.abs(originalOutlierRate - generatedOutlierRate) * 100));
    });
  }

  const avg = (arr) => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 80; // Default score if no numeric data
  
  return {
    varianceMatch: Math.max(0, Math.round(varianceMatch)),
    distributionSimilarity: Math.max(0, Math.round(avg(distributionScores))),
    edgeCaseCoverage: Math.max(0, Math.round(avg(edgeCaseScores))),
  };
}

// Generate synthetic data endpoint
app.post('/api/vertex-ai/generate', async (req, res) => {
  try {
    // MODIFIED: We no longer expect a schema in the request body.
    const { options = {} } = req.body;
    const { recordCount = 100, maskSensitive = true, sampleData = '' } = options;

    if (!sampleData) {
      return res.status(400).json({ success: false, error: 'Sample data is required for generation.' });
    }

    // 1. Infer the schema directly from the provided sample data
    const inferredSchema = inferSchemaFromSample(sampleData);
    console.log(`Inferred schema: ${inferredSchema.name}`);

    console.log(`Generating ${recordCount} records for schema: ${inferredSchema.name}`);
    
    // 2. Create the prompt using the newly inferred schema
    const prompt = createEnhancedDataGenerationPrompt(inferredSchema, { ...options, sampleData });

    // 1. Define a system instruction to lock in the model's behavior
    const systemInstruction = {
      role: 'system',
      parts: [{ text: `You are a highly precise data generation engine. Your sole purpose is to create structured JSON data that strictly adheres to the user's schema and examples. You must not deviate from the provided patterns. You will only output a valid JSON array.` }]
    };

    const generativeModel = vertexAI.getGenerativeModel({
      model: 'gemini-2.5-flash',
      systemInstruction: systemInstruction, // 2. Apply the system instruction
      generationConfig: {
        maxOutputTokens: 8192,
        temperature: 0.1, // 3. Lower temperature for maximum strictness
        topP: 0.8,
        topK: 40,
      },
    });

    console.log('Sending request to Vertex AI Gemini Flash...');
    
    const result = await generativeModel.generateContent(prompt);
    console.log('Raw result from Vertex AI:', JSON.stringify(result, null, 2));

    // Handle the response properly
    let text;
    if (result.response && typeof result.response.text === 'function') {
      text = result.response.text();
    } else if (result.response && result.response.candidates && result.response.candidates[0]) {
      const candidate = result.response.candidates[0];
      if (candidate.content && candidate.content.parts && candidate.content.parts[0]) {
        text = candidate.content.parts[0].text;
      } else {
        throw new Error('Unexpected response format from Vertex AI');
      }
    } else {
      console.log('Full result structure:', result);
      throw new Error('No valid response received from Vertex AI');
    }

    console.log('Extracted text:', text.substring(0, 200) + '...');
    
    const generatedData = parseGeneratedData(text);
    
    // 2. Calculate ACTUAL metrics after data is generated
    const metrics = calculateActualDataQualityMetrics(sampleData, generatedData);

    res.json({
      success: true,
      data: generatedData,
      recordCount: generatedData.length,
      schema: inferredSchema.name,
      model: 'gemini-2.5-flash',
      metrics: metrics // 3. Add metrics to the response
    });

  } catch (error) {
    console.error('Error generating synthetic data:', error);
    console.error('Error stack:', error.stack);
    res.status(500).json({
      success: false,
      error: error.message,
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

// Enhanced data generation prompt for Gemini
function createDataGenerationPrompt(schema, options) {
  const { recordCount = 100, maskSensitive = true } = options;
  
  return `You are a synthetic data generator. Generate ${recordCount} realistic test data records for a database schema.

SCHEMA INFORMATION:
Schema Name: ${schema.name}
Total Fields: ${schema.fields.length}

FIELD DEFINITIONS:
${schema.fields.map(field => {
    let definition = `- ${field.name}: ${field.type}`;
    if (field.sensitive) definition += ' [SENSITIVE - MASK WITH FAKE DATA]';
    if (field.constraints) definition += ` [Constraints: ${field.constraints.join(', ')}]`;
    return definition;
}).join('\n')}

DATA GENERATION RULES:
1. Generate exactly ${recordCount} records
2. Return ONLY a valid JSON array - no explanations, no markdown formatting
3. Each record must include ALL fields from the schema
4. For sensitive fields: Use realistic but completely fake data (fake names, emails, addresses, phone numbers)
5. For non-sensitive fields: Use realistic sample data appropriate for the field name and type
6. Ensure data relationships make sense (e.g., customer_id should be consistent if referenced multiple times)

DATA TYPE SPECIFICATIONS:
- integer: Whole numbers (e.g., 1, 100, 9999)
- string: Text values appropriate for the field name
- decimal: Numbers with decimal places (e.g., 99.99, 1250.50)
- date: Format as YYYY-MM-DD (e.g., 2023-01-15)
- timestamp: ISO 8601 format (e.g., 2023-01-15T10:30:00Z)

SENSITIVE DATA EXAMPLES:
- Names: Use common fake names like "John Smith", "Jane Doe"
- Emails: Use fake domains like "example.com", "test.org"
- Addresses: Use fake but realistic addresses
- Phone numbers: Use fake numbers in proper format

Generate the JSON array now:`;
}

// Enhanced data generation prompt for better quality
function createEnhancedDataGenerationPrompt(schema, options) {
  const { recordCount = 100, maskSensitive = true, sampleData = '' } = options;
  
  const sampleDataPromptSection = sampleData 
    ? `
**REFERENCE EXAMPLES (HIGHEST PRIORITY):**
The following are examples of the data to be generated. The structure, format, and data patterns shown here MUST be replicated precisely. Do not introduce new patterns.

\`\`\`
${sampleData}
\`\`\`
` 
    : '';

  return `
${sampleDataPromptSection}

**SCHEMA DEFINITION:**
-   **Record Count:** Generate exactly ${recordCount} records.
-   **Output Format:** JSON array of objects.
-   **Fields (per object):**
${schema.fields.map(field => {
    let spec = `    - ${field.name} (${field.type})`;
    if (field.sensitive && maskSensitive) spec += ' [SENSITIVE - MASK WITH FAKE DATA]';
    if (field.constraints) spec += ` [Constraints: ${field.constraints.join(', ')}]`;
    return spec;
}).join('\n')}

**FINAL INSTRUCTIONS:**
1.  Your entire output must be ONLY the JSON array. No text, no explanations, no markdown.
2.  If REFERENCE EXAMPLES are provided, they are the absolute source of truth for data style.
3.  Adhere strictly to the ${recordCount} record count.

JSON output:
`;
}

function parseGeneratedData(content) {
  try {
    console.log('Parsing content:', content.substring(0, 500) + '...');
    
    // More robust cleaning for Gemini responses
    let cleanContent = content.trim();
    
    // Remove markdown code blocks
    cleanContent = cleanContent.replace(/```json\s*/g, '');
    cleanContent = cleanContent.replace(/```\s*/g, '');
    
    // Remove any leading/trailing text before/after JSON
    const jsonStart = cleanContent.indexOf('[');
    const jsonEnd = cleanContent.lastIndexOf(']') + 1;
    
    if (jsonStart !== -1 && jsonEnd !== -1) {
      cleanContent = cleanContent.substring(jsonStart, jsonEnd);
    }
    
    console.log('Cleaned content for parsing:', cleanContent.substring(0, 200) + '...');
    
    const parsedData = JSON.parse(cleanContent);
    
    if (!Array.isArray(parsedData)) {
      throw new Error('Generated data is not an array');
    }
    
    console.log(`Successfully parsed ${parsedData.length} records`);
    return parsedData;
  } catch (error) {
    console.error('Error parsing generated data:', error);
    console.log('Raw content that failed to parse:', content);
    
    // Return some mock data as fallback
    const mockData = generateFallbackData(10);
    console.log('Returning fallback mock data instead');
    return mockData;
  }
}

// Fallback function to generate mock data if Vertex AI fails
function generateFallbackData(count) {
  const mockData = [];
  for (let i = 1; i <= count; i++) {
    mockData.push({
      customer_id: i,
      first_name: `TestUser${i}`,
      last_name: `Lastname${i}`,
      email: `testuser${i}@example.com`,
      phone: `(555) 123-${1000 + i}`,
      address: `${i} Test Street, Test City`,
      date_of_birth: `1990-01-${String(i % 28 + 1).padStart(2, '0')}`,
      account_balance: (Math.random() * 10000).toFixed(2),
      created_at: new Date().toISOString()
    });
  }
  return mockData;
}

app.listen(port, () => {
  console.log(`TDM Backend server running on port ${port}`);
  console.log(`Using Vertex AI Gemini models in project: ${PROJECT_ID}`);
});