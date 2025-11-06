const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs').promises;
const { google } = require('googleapis');
const vision = require('@google-cloud/vision');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcrypt');
const rateLimit = require('express-rate-limit');

const app = express();
const PORT = process.env.PORT || 3001;

// ========================================
// MIDDLEWARE
// ========================================

app.use(cors({
  origin: process.env.FRONTEND_URL || 'http://localhost:3000',
  credentials: true
}));

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ========================================
// GOOGLE CLOUD CONFIGURATION
// ========================================

let visionClient;
let sheets;
let authClient;

const GOOGLE_SHEET_ID = process.env.GOOGLE_SHEET_ID;
const JWT_SECRET = process.env.JWT_SECRET;

// Inizializza Google Cloud Services
async function initializeGoogleServices() {
  try {
    const credentials = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON || '{}');
    
    if (!credentials.private_key) {
      throw new Error('Google credentials mancanti');
    }

    // Vision API Client
    visionClient = new vision.ImageAnnotatorClient({
      credentials: credentials
    });

    // Google Sheets API Client
    authClient = new google.auth.JWT({
      email: credentials.client_email,
      key: credentials.private_key.replace(/\\n/g, '\n'),
      scopes: ['https://www.googleapis.com/auth/spreadsheets']
    });

    await authClient.authorize();
    
    sheets = google.sheets({ version: 'v4', auth: authClient });

    console.log('✅ Google Cloud Services inizializzati');
  } catch (error) {
    console.error('❌ Errore inizializzazione Google Services:', error);
    throw error;
  }
}

// ========================================
// MULTER CONFIGURATION
// ========================================

const storage = multer.diskStorage({
  destination: async (req, file, cb) => {
    const uploadDir = path.join(__dirname, 'uploads');
    try {
      await fs.mkdir(uploadDir, { recursive: true });
      cb(null, uploadDir);
    } catch (error) {
      cb(error);
    }
  },
  filename: (req, file, cb) => {
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
    cb(null, file.fieldname + '-' + uniqueSuffix + path.extname(file.originalname));
  }
});

const upload = multer({
  storage: storage,
  limits: { fileSize: 10 * 1024 * 1024 }, // 10MB
  fileFilter: (req, file, cb) => {
    if (file.mimetype === 'application/pdf' || file.mimetype.startsWith('image/')) {
      cb(null, true);
    } else {
      cb(new Error('Solo file PDF e immagini sono permessi'));
    }
  }
});

// ========================================
// AUTHENTICATION MIDDLEWARE
// ========================================

const authenticateToken = (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) {
    return res.status(401).json({ error: 'Token non fornito' });
  }

  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) {
      return res.status(403).json({ error: 'Token non valido' });
    }
    req.user = user;
    next();
  });
};

const requireAdmin = (req, res, next) => {
  if (req.user.ruolo !== 'admin') {
    return res.status(403).json({ error: 'Accesso negato: solo admin' });
  }
  next();
};

// ========================================
// RATE LIMITING
// ========================================

const loginLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minuti
  max: 5, // 5 tentativi
  message: 'Troppi tentativi di login, riprova tra 15 minuti'
});

// ========================================
// UTILITY FUNCTIONS
// ========================================

async function extractTextFromPDF(filePath) {
  try {
    const [result] = await visionClient.documentTextDetection(filePath);
    const fullTextAnnotation = result.fullTextAnnotation;
    return fullTextAnnotation ? fullTextAnnotation.text : '';
  } catch (error) {
    console.error('Errore estrazione testo:', error);
    throw error;
  }
}

function parseInvoiceData(text) {
  const lines = text.split('\n').map(line => line.trim()).filter(line => line);
  
  const data = {
    numeroFattura: '',
    dataConsegna: '',
    fornitore: '',
    totale: '',
    prodotti: [],
    iva: '',
    imponibile: ''
  };

  // Parsing logic
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    
    // Numero fattura
    if (line.match(/fattura|invoice|n\.|num/i)) {
      const match = line.match(/(\d{4,})/);
      if (match) data.numeroFattura = match[1];
    }
    
    // Data
    if (line.match(/data|date/i)) {
      const match = line.match(/(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/);
      if (match) data.dataConsegna = match[1];
    }
    
    // Totale
    if (line.match(/totale|total|importo/i)) {
      const match = line.match(/€?\s*(\d+[,\.]\d{2})/);
      if (match) data.totale = match[1].replace(',', '.');
    }
  }

  return data;
}

async function saveToGoogleSheet(invoiceData, puntoVendita) {
  try {
    const values = [[
      invoiceData.numeroFattura,
      invoiceData.dataConsegna,
      invoiceData.fornitore,
      puntoVendita,
      invoiceData.totale,
      'In Attesa',
      '',
      '',
      invoiceData.numeroColli || '',
      invoiceData.note || '',
      invoiceData.prodotti || '',
      invoiceData.iva || '',
      invoiceData.imponibile || '',
      invoiceData.testoDDT || ''
    ]];

    await sheets.spreadsheets.values.append({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: 'Sheet1!A:N',
      valueInputOption: 'RAW',
      resource: { values }
    });

    console.log('✅ Dati salvati su Google Sheet');
  } catch (error) {
    console.error('❌ Errore salvataggio Google Sheet:', error);
    throw error;
  }
}

// ========================================
// AUTH ENDPOINTS
// ========================================

app.post('/api/auth/login', loginLimiter, async (req, res) => {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(400).json({ error: 'Email e password richiesti' });
    }

    // Leggi utenti dal Google Sheet
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: 'Utenti!A2:E'
    });

    const users = response.data.values || [];
    const user = users.find(u => u[0] === email);

    if (!user) {
      return res.status(401).json({ error: 'Credenziali non valide' });
    }

    const [userEmail, hashedPassword, nome, ruolo, puntoVendita] = user;
    const isValidPassword = await bcrypt.compare(password, hashedPassword);

    if (!isValidPassword) {
      return res.status(401).json({ error: 'Credenziali non valide' });
    }

    const token = jwt.sign(
      { email: userEmail, ruolo, puntoVendita },
      JWT_SECRET,
      { expiresIn: '24h' }
    );

    res.json({
      token,
      user: {
        email: userEmail,
        nome,
        ruolo,
        puntoVendita
      }
    });
  } catch (error) {
    console.error('Errore login:', error);
    res.status(500).json({ error: 'Errore durante il login' });
  }
});

app.get('/api/auth/verify', authenticateToken, (req, res) => {
  res.json({
    valid: true,
    user: {
      email: req.user.email,
      ruolo: req.user.ruolo,
      puntoVendita: req.user.puntoVendita
    }
  });
});

app.post('/api/auth/logout', authenticateToken, (req, res) => {
  res.json({ message: 'Logout effettuato con successo' });
});

// ========================================
// INVOICE ENDPOINTS
// ========================================

app.get('/api/invoices', authenticateToken, async (req, res) => {
  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: 'Sheet1!A2:N'
    });

    const rows = response.data.values || [];
    const userPuntoVendita = req.user.puntoVendita;

    const invoices = rows
      .filter(row => row[3] === userPuntoVendita)
      .map((row, index) => ({
        id: index,
        numeroFattura: row[0],
        dataConsegna: row[1],
        fornitore: row[2],
        puntoVendita: row[3],
        totale: row[4],
        consegnato: row[5] === 'Consegnato',
        dataConferma: row[6] || null,
        confermaDa: row[7] || null,
        testoDDT: row[13] || ''
      }));

    res.json(invoices);
  } catch (error) {
    console.error('Errore caricamento fatture:', error);
    res.status(500).json({ error: 'Errore nel caricamento delle fatture' });
  }
});

app.post('/api/invoices/:id/confirm', authenticateToken, async (req, res) => {
  try {
    const invoiceId = parseInt(req.params.id);
    const userEmail = req.user.email;
    const now = new Date().toLocaleString('it-IT');

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: 'Sheet1!A2:N'
    });

    const rows = response.data.values || [];
    const rowIndex = invoiceId + 2;

    await sheets.spreadsheets.values.update({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: `Sheet1!F${rowIndex}:H${rowIndex}`,
      valueInputOption: 'RAW',
      resource: {
        values: [['Consegnato', now, userEmail]]
      }
    });

    res.json({ 
      success: true, 
      message: 'Fattura confermata con successo',
      dataConferma: now,
      confermaDa: userEmail
    });
  } catch (error) {
    console.error('Errore conferma fattura:', error);
    res.status(500).json({ error: 'Errore nella conferma della fattura' });
  }
});

// ========================================
// MOVIMENTAZIONI ENDPOINTS
// ========================================

app.get('/api/movimentazioni', authenticateToken, async (req, res) => {
  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: 'Movimentazioni!A2:G'
    });

    const rows = response.data.values || [];
    const userPuntoVendita = req.user.puntoVendita;

    const movimentazioni = rows
      .filter(row => row[1] === userPuntoVendita || row[2] === userPuntoVendita)
      .map((row, index) => ({
        id: index,
        data: row[0],
        da: row[1],
        a: row[2],
        prodotto: row[3],
        quantita: row[4],
        note: row[5] || '',
        stato: row[6] || 'In corso'
      }));

    res.json(movimentazioni);
  } catch (error) {
    console.error('Errore caricamento movimentazioni:', error);
    res.status(500).json({ error: 'Errore nel caricamento delle movimentazioni' });
  }
});

app.post('/api/movimentazioni', authenticateToken, async (req, res) => {
  try {
    const { da, a, prodotto, quantita, note } = req.body;
    const data = new Date().toLocaleDateString('it-IT');

    const values = [[
      data,
      da,
      a,
      prodotto,
      quantita,
      note || '',
      'In corso'
    ]];

    await sheets.spreadsheets.values.append({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: 'Movimentazioni!A:G',
      valueInputOption: 'RAW',
      resource: { values }
    });

    res.json({ 
      success: true, 
      message: 'Movimentazione creata con successo',
      movimentazione: {
        data,
        da,
        a,
        prodotto,
        quantita,
        note,
        stato: 'In corso'
      }
    });
  } catch (error) {
    console.error('Errore creazione movimentazione:', error);
    res.status(500).json({ error: 'Errore nella creazione della movimentazione' });
  }
});

// ========================================
// PRODOTTI ENDPOINTS
// ========================================

app.get('/api/prodotti', authenticateToken, async (req, res) => {
  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: 'Prodotti!A2:F'
    });

    const rows = response.data.values || [];

    const prodotti = rows.map((row, index) => ({
      id: index,
      codice: row[0],
      nome: row[1],
      categoria: row[2],
      unitaMisura: row[3],
      prezzoUnitario: row[4],
      fornitore: row[5]
    }));

    res.json(prodotti);
  } catch (error) {
    console.error('Errore caricamento prodotti:', error);
    res.status(500).json({ error: 'Errore nel caricamento dei prodotti' });
  }
});

// ========================================
// ADMIN ENDPOINTS
// ========================================

app.get('/api/admin/dashboard', authenticateToken, requireAdmin, async (req, res) => {
  try {
    // Fatture
    const invoicesResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: 'Sheet1!A2:N'
    });
    const invoices = invoicesResponse.data.values || [];
    const pendingInvoices = invoices.filter(row => row[5] !== 'Consegnato');
    const confirmedInvoices = invoices.filter(row => row[5] === 'Consegnato');

    // Movimentazioni
    const movimentazioniResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: 'Movimentazioni!A2:G'
    });
    const allMovimentazioni = movimentazioniResponse.data.values || [];

    // Utenti
    const usersResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: 'Utenti!A2:E'
    });
    const users = usersResponse.data.values || [];

    // Negozi
    const storesResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: 'Negozi!A2:D'
    });
    const stores = storesResponse.data.values || [];

    const stats = {
      invoices: {
        total: invoices.length,
        pending: pendingInvoices.length,
        confirmed: confirmedInvoices.length
      },
      movimentazioni: {
        total: allMovimentazioni.length
      },
      users: {
        total: users.length
      },
      stores: {
        total: stores.length
      }
    };

    res.json(stats);
  } catch (error) {
    console.error('Errore nel caricamento dashboard admin:', error);
    res.status(500).json({ error: 'Errore nel caricamento della dashboard' });
  }
});

// ========================================
// ADMIN: GET ALL INVOICES WITH DDT
// ========================================
app.get('/api/admin/invoices', authenticateToken, requireAdmin, async (req, res) => {
  try {
    console.log('📋 Admin: Caricamento fatture globali...');
    
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: 'Sheet1!A2:N',
    });

    const rows = response.data.values || [];
    
    if (rows.length === 0) {
      console.log('⚠️ Nessuna fattura trovata nel Google Sheet');
      return res.json([]);
    }

    const invoices = rows
      .map((row, index) => {
        try {
          // Validazione base
          if (!row[0]) {
            console.log(`⚠️ Riga ${index + 2} ignorata: manca numero fattura`);
            return null;
          }

          return {
            id: index + 1,
            numeroFattura: String(row[0] || '').trim(),
            dataConsegna: String(row[1] || '').trim(),
            fornitore: String(row[2] || '').trim(),
            puntoVendita: String(row[3] || '').trim(),
            totale: String(row[4] || '0').trim(),
            consegnato: ['Consegnato', 'TRUE', 'true', true].includes(row[5]),
            dataConferma: row[6] || null,
            confermaDa: row[7] || null,
            numeroColli: String(row[8] || '').trim(),
            note: String(row[9] || '').trim(),
            prodotti: String(row[10] || '').trim(),
            iva: String(row[11] || '').trim(),
            imponibile: String(row[12] || '').trim(),
            testoDDT: String(row[13] || 'Nessun DDT disponibile').trim(),
          };
        } catch (error) {
          console.error(`❌ Errore parsing riga ${index + 2}:`, error.message);
          return null;
        }
      })
      .filter(inv => inv !== null);

    // Log statistiche
    const consegnate = invoices.filter(inv => inv.consegnato).length;
    const inAttesa = invoices.filter(inv => !inv.consegnato).length;
    
    console.log(`✅ Fatture caricate: ${invoices.length} (${consegnate} consegnate, ${inAttesa} in attesa)`);

    res.json(invoices);
  } catch (error) {
    console.error('❌ Errore caricamento fatture admin:', error);
    res.status(500).json({ 
      error: 'Errore nel caricamento delle fatture',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

app.get('/api/admin/movimentazioni', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: 'Movimentazioni!A2:G'
    });

    const rows = response.data.values || [];

    const movimentazioni = rows.map((row, index) => ({
      id: index,
      data: row[0],
      da: row[1],
      a: row[2],
      prodotto: row[3],
      quantita: row[4],
      note: row[5] || '',
      stato: row[6] || 'In corso'
    }));

    res.json(movimentazioni);
  } catch (error) {
    console.error('Errore caricamento movimentazioni admin:', error);
    res.status(500).json({ error: 'Errore nel caricamento delle movimentazioni' });
  }
});

app.get('/api/admin/stores', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: 'Negozi!A2:D'
    });

    const rows = response.data.values || [];

    const stores = rows.map((row, index) => ({
      id: index,
      nome: row[0],
      indirizzo: row[1],
      citta: row[2],
      attivo: row[3] === 'TRUE' || row[3] === true
    }));

    res.json(stores);
  } catch (error) {
    console.error('Errore caricamento negozi:', error);
    res.status(500).json({ error: 'Errore nel caricamento dei negozi' });
  }
});

app.get('/api/admin/users', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: 'Utenti!A2:E'
    });

    const rows = response.data.values || [];

    const users = rows.map((row, index) => ({
      id: index,
      email: row[0],
      nome: row[2] || '',
      ruolo: row[3] || 'user',
      negozio: row[4] || '',
      createdAt: new Date().toISOString()
    }));

    res.json(users);
  } catch (error) {
    console.error('Errore caricamento utenti:', error);
    res.status(500).json({ error: 'Errore nel caricamento degli utenti' });
  }
});

app.get('/api/admin/export', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: 'Sheet1!A2:N'
    });

    const rows = response.data.values || [];
    
    const csv = [
      'Numero Fattura,Data Consegna,Fornitore,Punto Vendita,Totale,Stato,Data Conferma,Confermato Da,Numero Colli,Note,Prodotti,IVA,Imponibile,DDT',
      ...rows.map(row => row.join(','))
    ].join('\n');

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename=export-fatture.csv');
    res.send(csv);
  } catch (error) {
    console.error('Errore export:', error);
    res.status(500).json({ error: 'Errore durante l\'export' });
  }
});

// ========================================
// TXT FILES ENDPOINTS
// ========================================

app.get('/api/txt-files', authenticateToken, async (req, res) => {
  try {
    const txtDir = path.join(__dirname, 'txt-files');
    await fs.mkdir(txtDir, { recursive: true });
    
    const files = await fs.readdir(txtDir);
    const txtFiles = files.filter(f => f.endsWith('.txt'));
    
    const filesList = await Promise.all(
      txtFiles.map(async (filename) => {
        const filePath = path.join(txtDir, filename);
        const stats = await fs.stat(filePath);
        return {
          filename,
          date: stats.mtime,
          size: stats.size
        };
      })
    );

    res.json(filesList.sort((a, b) => b.date - a.date));
  } catch (error) {
    console.error('Errore lettura file TXT:', error);
    res.status(500).json({ error: 'Errore nella lettura dei file' });
  }
});

app.get('/api/txt-files/:filename', authenticateToken, async (req, res) => {
  try {
    const filename = req.params.filename;
    const filePath = path.join(__dirname, 'txt-files', filename);
    
    const stats = await fs.stat(filePath);
    
    res.json({
      filename,
      date: stats.mtime,
      size: stats.size,
      path: filePath
    });
  } catch (error) {
    console.error('Errore lettura file:', error);
    res.status(404).json({ error: 'File non trovato' });
  }
});

app.get('/api/txt-files/:filename/content', authenticateToken, async (req, res) => {
  try {
    const filename = req.params.filename;
    const filePath = path.join(__dirname, 'txt-files', filename);
    
    const content = await fs.readFile(filePath, 'utf-8');
    
    res.json({ content });
  } catch (error) {
    console.error('Errore lettura contenuto file:', error);
    res.status(404).json({ error: 'File non trovato' });
  }
});

app.get('/api/txt-files/download-by-date/:date', authenticateToken, async (req, res) => {
  try {
    const requestedDate = req.params.date;
    const txtDir = path.join(__dirname, 'txt-files');
    
    const files = await fs.readdir(txtDir);
    const matchingFiles = files.filter(f => f.includes(requestedDate) && f.endsWith('.txt'));
    
    if (matchingFiles.length === 0) {
      return res.status(404).json({ error: 'Nessun file trovato per questa data' });
    }
    
    const filePath = path.join(txtDir, matchingFiles[0]);
    res.download(filePath);
  } catch (error) {
    console.error('Errore download file:', error);
    res.status(500).json({ error: 'Errore nel download del file' });
  }
});

app.get('/api/txt-files/stats-by-date', authenticateToken, async (req, res) => {
  try {
    const txtDir = path.join(__dirname, 'txt-files');
    const files = await fs.readdir(txtDir);
    const txtFiles = files.filter(f => f.endsWith('.txt'));
    
    const statsByDate = {};
    
    for (const filename of txtFiles) {
      const dateMatch = filename.match(/(\d{4}-\d{2}-\d{2})/);
      if (dateMatch) {
        const date = dateMatch[1];
        if (!statsByDate[date]) {
          statsByDate[date] = { count: 0, files: [] };
        }
        statsByDate[date].count++;
        statsByDate[date].files.push(filename);
      }
    }
    
    res.json(statsByDate);
  } catch (error) {
    console.error('Errore statistiche file:', error);
    res.status(500).json({ error: 'Errore nel calcolo delle statistiche' });
  }
});

// ========================================
// HEALTH & INFO
// ========================================

app.get('/api/health', (req, res) => {
  res.json({
    status: 'OK',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    environment: process.env.NODE_ENV || 'development'
  });
});

app.get('/api/info', authenticateToken, (req, res) => {
  res.json({
    version: '1.0.0',
    user: {
      email: req.user.email,
      ruolo: req.user.ruolo,
      puntoVendita: req.user.puntoVendita
    }
  });
});

// ========================================
// ERROR HANDLING
// ========================================

app.use((err, req, res, next) => {
  console.error('Errore:', err);
  res.status(500).json({
    error: 'Errore interno del server',
    message: process.env.NODE_ENV === 'development' ? err.message : undefined
  });
});

// ========================================
// SERVER START
// ========================================

async function startServer() {
  try {
    await initializeGoogleServices();
    
    app.listen(PORT, '0.0.0.0', () => {
      console.log(`\n🚀 Server avviato su porta ${PORT}`);
      console.log(`📍 Environment: ${process.env.NODE_ENV || 'development'}`);
      console.log(`🔗 API: http://localhost:${PORT}`);
      console.log(`✅ Google Services: Pronti\n`);
    });
  } catch (error) {
    console.error('❌ Errore avvio server:', error);
    process.exit(1);
  }
}

startServer();
