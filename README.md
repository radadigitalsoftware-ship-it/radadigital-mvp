# RadaDigital

## Arranque con radar AIS (proxy backend)

La clave de AISstream vive solo en el servidor (`server/.env`), no en el navegador.

```bash
cd server
npm install
npm start
```

Abrí **http://localhost:3000** e iniciá sesión. El indicador **RADAR EN VIVO** aparece cuando el proxy está conectado a AISstream.

### Variables (`server/.env`)

| Variable | Descripción |
|----------|-------------|
| `AISSTREAM_API_KEY` | Clave de [aisstream.io](https://aisstream.io) |
| `PORT` | Puerto HTTP (por defecto `3000`) |
