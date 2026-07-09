# Plataforma de Análise EUR/USD (SMC + Tick Engine)

Esta é a base do backend de ultra-baixa latência para a Plataforma de Análise Gráfica Profissional focada no par EUR/USD. Construído em Rust com Tokio, o sistema foi projetado para processar dados de mercado em regime de streaming (HFT) sem latência de Garbage Collection, usando estruturas concorrentes (Ring Buffers) alocadas antecipadamente.

## Arquitetura

O pipeline de dados (Tick Ingestion Engine) flui da seguinte maneira:

**Provider Forex** (externo) 
↓ (tick bruto: bid/ask/ts)
**Feed Handler** (normaliza preço para micropips precisos)
↓ (TickEnvelope)
**Sequencer** (valida monotonicidade temporal, deduplica, atribui sequencial)
↓ (stream validado e ordenado)
**Tick Store / Ring Buffer** (memória circular lock-free de alta performance)
↓
*(Futuro) Multi-Timeframe Aggregator & Broadcast Hub (WebSocket)*

## Decisões Críticas de Design

1. **Preços em Micropips (Inteiros)**
   Todo o fluxo interno abandona tipos de ponto flutuante (`f64`) na porta de entrada (Feed Handler) e usa `i64` escalonado em `1_000_000` (micropips). Isso elimina o "drift" e erros de precisão na construção de velas.

2. **Ring Buffers e Zero Alocação**
   O buffer histórico (Tick Store) utiliza `Vec` com capacidade fixada na inicialização e sobrescrita circular. Em regime permanente de recepção de ticks, o sistema realiza **zero chamadas de alocação de memória**, garantindo previsibilidade de latência e nenhuma pausa de GC (inexistente em Rust).

3. **Protocol Buffers e WebSockets**
   A comunicação será baseada em payloads binários do Protobuf 3 empacotados em frames WebSocket. É vastamente mais leve e desserializado mais rápido do que JSON.

## Estrutura de Diretórios (Iteração 1)

```
plataforma-analise/
├── Cargo.toml                          # Workspace root
├── proto/
│   ├── tick.proto                      # TickEnvelope schema
│   └── market_stream.proto             # Mensagens WebSocket
├── backend/
│   ├── contracts-rs/                   # Crate de Protobuf -> Rust
│   ├── common/                         # Tipos, erros, config base
│   ├── aggregation-core/               # Estruturas do Ring Buffer e Sequencer
│   └── feed-handler/                   # Conector abstrato e lógica de normalização
└── README.md
```

## Como Construir e Testar

Requisitos:
- Rust (Cargo) 1.70+
- Protoc (Protobuf Compiler) instalado no PATH

Na raiz do projeto:

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace
```
