# Dicionário de dados — ERP (carga inicial e extração diária sintéticas)

Dados que chegam na **landing** para a fatia 1, como o ERP da Serra Azul os entregaria no go-live e depois dele, na extração diária (§5). É a "matéria-prima" da bronze: nomes em português, como a fonte manda (a tradução para inglês acontece na silver, ADR-0004).

As seções 1 a 4 descrevem a **carga inicial**; as contagens delas são as da carga. A **extração diária** está na §5.

| Item | Valor |
|---|---|
| Período coberto pela carga inicial | 2025-04-19 a 2026-10-18 (18 meses, até D-1) |
| Data da extração da carga inicial | 2026-10-19 (pasta de cada dataset) |
| Formato | Parquet, em `/Volumes/core/landing/erp/<dataset>/<AAAA-MM-DD>/` e `/Volumes/core/landing/ref/cep_municipio/2026-10-19/` |
| Gerador da carga inicial | [`data_gen/erp/erp_initial_load.py`](../data_gen/erp/erp_initial_load.py), job `erp_initial_load` do bundle. **Não rodar de novo na landing real** (ADR-0012 D4) |
| Extração diária | Desde 2026-10-20, uma por dia: [`data_gen/erp/erp_daily_extraction.py`](../data_gen/erp/erp_daily_extraction.py), tarefa `simulate_erp_extraction` do job `serra_azul_daily` (§5, ADR-0012) |
| Gabaritos | `data_gen/erp/manifest_<dataset>.json`: contagem exata de cada sujeira injetada na carga inicial |
| Reprodutível? | Sim. Toda aleatoriedade vem de hash da chave + semente `20261019`, e o Faker tem versão fixa (`faker==37.12.0`). Rodar de novo gera o mesmo conteúdo; cada extração diária é função só da sua data |

---

## 1. Como as tabelas se relacionam

```mermaid
erDiagram
    cep_municipio ||--o{ clientes_pdv : "cep"
    clientes_pdv ||--o{ pedidos_cab : "cod_cliente (+ data)"
    hierarquia_comercial ||--o{ pedidos_cab : "cod_vendedor (+ data)"
    pedidos_cab ||--|{ pedidos_itens : "num_pedido"
    produtos ||--o{ pedidos_itens : "cod_produto (+ data)"
    pedidos_cab ||--o{ notas_fiscais : "num_pedido"
    notas_fiscais ||--|{ notas_fiscais_itens : "num_nf"
    pedidos_itens ||--o{ notas_fiscais_itens : "num_pedido + item_pedido"
    notas_fiscais ||--o| notas_fiscais : "num_nf_origem (devolução)"
```

**Três tabelas são versionadas:** `clientes_pdv`, `produtos` e `hierarquia_comercial` têm uma linha por versão. A ligação com pedidos e NFs é pelo código **e pela data**: vale a versão cuja vigência contém a data do evento. A data é a do documento que carrega o código: o vendedor está no pedido, então o item de NF usa a data do pedido; o cliente está na NF, então usa a data de emissão (ADR-0010).

**As outras cinco são transacionais ou de referência:** `pedidos_cab`, `pedidos_itens`, `notas_fiscais` e `notas_fiscais_itens` são transacionais; `cep_municipio` é referência. Nenhuma delas é versionada.

### Como ler a vigência

O ERP usa **intervalo fechado**:
- **Início:** `dt_inicio_vigencia` é o primeiro dia em que a versão vale.
- **Fim:** `dt_fim_vigencia` é o **último** dia, o da véspera da próxima versão. Fica nulo na versão vigente.

Na silver, isso vira o intervalo semiaberto `[valid_from, valid_to)` do ADR-0006: `valid_to` = dia seguinte ao fim, ou `9999-12-31` para a versão vigente.

---

## 2. Regras de negócio que atravessam as tabelas

### Canais de venda

| `tipo_cliente` | Canal | Clientes | % do volume (hl) | Como compra |
|---|---|---|---|---|
| `DISTRIBUIDOR` | Distribuidor | 12 | 55,0% | Poucos pedidos enormes (~40 itens, centenas de caixas por item); preço ~26% abaixo da tabela |
| `REDE` | Rede varejista | 2 | 26,2% | ~8 pedidos por dia, um por centro de distribuição da rede; preço ~18% abaixo |
| `PDV` | Venda direta | 39.759 | 18,8% | Um pedido a cada ~4 semanas em média, com poucos itens e poucas caixas; preço cheio |

O **segmento** (`BAR`, `RESTAURANTE`, `MERCADO`, `CONVENIENCIA`, `ADEGA`) é o tipo de estabelecimento do PDV. É ele que muda quando o caso de negócio fala em "mudança de canal". No dashboard, porém, "canal" é o `sales_channel` (DIRETO, DISTRIBUIDOR, REDE), derivado do `tipo_cliente`; a mudança de segmento fica no `trade_segment` (ADR-0010). Bares e restaurantes compram mais garrafa retornável e barril; mercados, conveniências e adegas compram mais lata e PET.

### Regiões: geográfica ≠ comercial

- **Região geográfica:** vem da UF do cliente (SUDESTE, SUL, NORDESTE, CENTRO-OESTE, NORTE). A UF está no cadastro; a região é derivada na silver.
- **Região comercial:** vem do vendedor do pedido (`hierarquia_comercial.regiao_comercial`). Tem uma sexta região, **CONTAS NACIONAIS**, que atende as duas redes.

Elas divergem de propósito. As redes têm sede em SP, então **o Sudeste fica com 56% do volume geográfico**, mas na visão comercial esse volume é de Contas Nacionais. Por isso o dashboard usa a região comercial (ADR-0006).

### Ciclo de vida do pedido (`pedidos_cab.status`)

```
ABERTO ──faturamento (D+0 a D+2)──► FATURADO
   │                                   │
   │ (5% dos pedidos)                  └─cancelado depois (0,3%)─► CANCELADO + NF de devolução
   ├──► PARCIALMENTE FATURADO ──2ª NF (1 a 3 dias)──► FATURADO
   └─cancelado antes de faturar (2%)──► CANCELADO (sem NF)
```

| Status final | Pedidos |
|---|---|
| `FATURADO` | 786.692 |
| `CANCELADO` | 18.190 (2.200 depois de faturar) |
| `ABERTO` | 141 (pedidos dos últimos dias, ainda sem NF) |
| `PARCIALMENTE FATURADO` | 130 (aguardando a 2ª NF) |

### Notas fiscais e CFOP

O tipo da NF **não** vem numa coluna própria: vem do **CFOP** (Código Fiscal de Operações e Prestações), como num ERP real.

| CFOP | Operação | Quando |
|---|---|---|
| `5102` | Venda dentro do estado | Cliente na mesma UF da fábrica que atende |
| `6102` | Venda para outro estado | Cliente em outra UF |
| `1202` | Devolução de venda, dentro do estado | Devolução de cliente da mesma UF |
| `2202` | Devolução de venda, de outro estado | Devolução de cliente de outra UF |

**Fábrica que atende cada região:**
- **Feira de Santana (BA):** o Nordeste.
- **Itu (SP):** o estado de SP, o Sul e o Centro-Oeste.
- **Contagem (MG):** o restante do Sudeste e o Norte.

Na NF de devolução, os valores vêm **positivos**, como no documento fiscal. A silver os mantém positivos; o sinal negativo é aplicado só no `fact_invoice`, uma única vez (ADR-0003, ADR-0010).

### Preço, desconto e valor

- `vl_unitario`: preço por **unidade de venda** (o SKU: uma caixa, pack ou barril), já com o preço do canal. Os preços sobem 6% em 2026-01-01 (reajuste anual).
- `vl_desconto`: desconto promocional da linha, em reais.
  - Médias: ~3% para PDV, ~6% para distribuidor, ~9% para rede.
  - A Pilsen no Sul tem ~9% fora da guerra de preço.
- `vl_total_item` = `quantidade × vl_unitario − vl_desconto`.
- Os valores são brutos de impostos (incluem os tributos da NF). Receita em 18 meses: **R$ 3,92 bi** (~R$ 2,6 bi/ano).

### Volume em hectolitros

A indústria de bebidas mede volume em **hectolitros (hl = 100 litros)**, não em caixas: uma caixa de 24 latas e um barril de 50 L não são comparáveis em "unidades".

- **Litros por SKU:** `volume_ml × unidades_pack / 1000`.
- **Hectolitros:** `quantidade × litros por SKU / 100`.

Total gerado: **4,2 mi hl** em 18 meses (~2,8 mi hl/ano).

### Calendário e sazonalidade

- **Dia da semana:** pedidos B2B concentram-se em dias úteis; sábado é fraco e domingo quase zero.
- **Pico de verão:** dezembro (302 mil hl, o maior mês) e fevereiro. Antes do Carnaval de 2026 (14 a 17/02), os clientes se abastecem; durante a festa, quase não há pedidos.
- **Inverno:** é baixa, e mais acentuada no Sul.
- **Festas juninas:** junho tem pico no Nordeste.
- **Feriados nacionais:** derrubam os pedidos do dia.

### As quatro histórias embutidas (valores medidos nos dados)

| História | O que os dados mostram |
|---|---|
| **Expansão no Nordeste** | Volume de PDV no NE +77% (abr–jun/2026 vs abr–jun/2025), contra +16% no SE. Crescem a carteira (mais PDVs novos) e a frequência |
| **Lançamento da Névoa Zero** (set/2025) | 2,4% do volume no mês de lançamento → 7,1% em set/2026 |
| **Reorganização de julho/2026** | 6 vendedores do NE passam para o SE. Receita de jan–jun/2026 do NE: R$ 209,2 mi "como era" vs R$ 197,4 mi "como é hoje" (−5,6%). Mais R$ 6,6 mi de vendedores desligados não têm versão atual |
| **Guerra de preço da Pilsen no Sul** (3º tri/2026) | Desconto médio de 9,2% → 19,2%; volume +37% sobre o 3º tri/2025 |

---

## 3. Datasets

**Legenda da coluna "Silver":** nome previsto da coluna na silver (convenções do ADR-0004). **⚠ PII** marca uma coluna com dado pessoal, que **não sai da bronze**.

### 3.1 `clientes_pdv`: cadastro de clientes (versionado)

**Grão:** 1 linha por cliente por versão de cadastro. **Chave:** `cod_cliente` + `dt_inicio_vigencia`. **Extração:** full, com todo o histórico. **Linhas:** 72.650 (39.773 clientes).

| Coluna | Tipo | Significado e regra | Silver |
|---|---|---|---|
| `cod_cliente` | string | Código do cliente no ERP (`C000001`…). Nunca muda nem é reaproveitado | `customer_id` |
| `tipo_cliente` | string | `PDV`, `DISTRIBUIDOR` ou `REDE`: define o canal de venda | `customer_type` → `sales_channel` |
| `segmento` | string | Tipo de estabelecimento do PDV; `DISTRIBUIDOR` / `SUPERMERCADO` para os demais. Pode mudar entre versões | `trade_segment` |
| `tipo_pessoa` | string | `J` (CNPJ) ou `F` (CPF; 3.143 PDVs são pessoa física) | `person_type` |
| `cnpj_cpf` | string | Documento, só dígitos (14 ou 11). CNPJ alfanumérico ainda não previsto no contrato ([ADR-0009](adr/0009-validacao-de-documento-digitos-repetidos-e-cnpj-alfanumerico.md)). ⚠ PII | só `is_document_valid` (módulo 11; dígitos todos iguais = inválido) |
| `razao_social` | string | Nome jurídico. Quando a pessoa é física, é o nome da pessoa. ⚠ PII para CPF | `legal_name` (só PJ) |
| `nome_fantasia` | string | Nome comercial (ex.: "BAR DO …", "MERCADO …") | `trade_name` |
| `nome_responsavel`, `email`, `telefone` | string | Contato do responsável. ⚠ PII | não vão |
| `cep` | string | CEP no formato `00000-000`; chave para a referência de município | `postal_code` |
| `logradouro`, `numero`, `bairro` | string | Endereço | `street`, `street_number`, `district` |
| `municipio` | string | Cidade **como digitada**: pode vir com grafia alternativa | `city` (pela referência de CEP) |
| `uf` | string | Estado (sigla) | `state` → `geo_region` |
| `status` | string | `ATIVO` / `INATIVO` | `status` |
| `dt_cadastro` | date | Data de criação do cliente | `registered_date` |
| `dt_inicio_vigencia`, `dt_fim_vigencia` | date | Vigência da versão (intervalo fechado) | `valid_from`, `valid_to` |
| `dt_atualizacao` | timestamp | Quando a versão foi gravada no ERP | `_source_updated_at` |

| Sujeira | Quantidade | Tratamento previsto |
|---|---|---|
| Documento com dígito verificador inválido | 735 clientes | Flag `is_document_valid` na silver; o cliente não é descartado |
| Cidade com grafia diferente da do CEP ("Sta Luzia", "Sao Paulo", "B. Horizonte"…) | 7.264 versões | Cidade substituída pela da referência de CEP, **antes** de calcular versões |
| Versões que só mudam contato, razão social, status ou grafia | 72.650 versões → 47.166 na `dim_customer` | Fundidas: só atributos rastreados geram versão (ADR-0006) |
| Clientes que ainda não entraram no cadastro (pedido chega antes) | 227 clientes / 313 pedidos | FK órfã → membro `-1` na gold; corrige quando o cadastro chegar. O cadastro deles chega nas extrações diárias de 2026-10-20 a 2026-10-29 (§5). Nos fatos, com `_is_customer_id_known = false`: em 2026-10-21, 1.070 itens no `fact_sales` e 1.084 no `fact_invoice` (ADR-0010); em 2026-10-27, depois de 8 extrações diárias, 254 e 256 (ADR-0012) |

### 3.2 `produtos`: cadastro de produtos (versionado)

**Grão:** 1 linha por produto por versão. **Chave:** `cod_produto` + `dt_inicio_vigencia`. **Extração:** full. **Linhas:** 126 (92 produtos, 89 SKUs, 82 ativos no fim).

| Coluna | Tipo | Significado e regra | Silver |
|---|---|---|---|
| `cod_produto` | string | Id interno do ERP (`P00001`…), **nunca reaproveitado**: é a identidade do produto (ADR-0006) | `product_id` |
| `sku` | string | Código comercial (6 dígitos). **Pode ser reaproveitado** para outro produto | `sku` |
| `ean` | string | Código de barras EAN-13 (prefixo 789 = Brasil) | `ean` |
| `descricao` | string | Descrição; mudou de padrão em 2025-11-01 ("CERV SA PILSEN LT350 C/12" → "CERVEJA SERRA AZUL PILSEN LATA 350ML PACK 12") | `description` |
| `marca` | string | 5 marcas: SERRA AZUL PILSEN, SERRA AZUL PURO MALTE, ALVORADA, NEVOA ZERO, GUARA SERRA | `brand` |
| `categoria` | string | CERVEJA MAINSTREAM, CERVEJA PREMIUM, CERVEJA ARTESANAL, CERVEJA SEM ALCOOL, REFRIGERANTE | `category` |
| `liquido` | string | Estilo (PILSEN, PURO MALTE, IPA, WEISS, APA, PILSEN ZERO, GUARANA) | `liquid` |
| `embalagem` | string | LATA, LONG NECK, GARRAFA RETORNAVEL, BARRIL, PET | `package_type` |
| `volume_ml` | int | Volume de cada unidade (269, 350, 473, 330, 600, 2000, 30000, 50000) | `unit_volume_ml` |
| `unidades_pack` | int | Unidades por SKU (1 para barril e PET unitário) | `units_per_pack` |
| `status` | string | `ATIVO` / `DESCONTINUADO` | `status` |
| `dt_lancamento` | date | Início de vendas (sempre dia 1) | `launch_date` |
| `dt_descontinuacao` | date | Último dia de vendas; preenchido só na versão "DESCONTINUADO" | `discontinued_date` |
| `dt_inicio_vigencia`, `dt_fim_vigencia`, `dt_atualizacao` | date / timestamp | Vigência e gravação da versão | `valid_from`, `valid_to`, `_source_updated_at` |

**Derivada na silver:** `liters_per_sku = volume_ml × unidades_pack / 1000`.

| Sujeira | Quantidade | Tratamento previsto |
|---|---|---|
| SKU reaproveitado: promoção de verão descontinuada em fev/2026 e SKU reusado em produto novo em mai/2026 | 3 SKUs (`100536`, `100549`, `100551`) | Identidade por `cod_produto`; o SKU vira atributo |
| EAN duplicado entre produtos diferentes | 2 pares | Não é chave: a identidade é o `product_id`. Sem regra de qualidade própria |
| Descrição alterada (padronização) | 24 produtos | Tipo 1: vale a descrição atual |

**Lançamentos na janela:**
- 12 SKUs da Névoa Zero (set/2025);
- 4 promoções de verão (dez/2025 a fev/2026);
- 3 produtos novos (mai/2026).

**Descontinuados:** 10.

### 3.3 `hierarquia_comercial`: estrutura de vendas (versionada)

**Grão:** 1 linha por vendedor por vigência. **Chave:** `cod_vendedor` + `dt_inicio_vigencia`. **Extração:** full. **Linhas:** 505 (146 vendedores; 140 com versão vigente).

| Coluna | Tipo | Significado e regra | Silver |
|---|---|---|---|
| `cod_vendedor` | string | Código do vendedor (`V0001`…) | `sales_rep_id` |
| `nome_vendedor` | string | Nome do vendedor (funcionário) | `sales_rep_name` |
| `cod_supervisor`, `nome_supervisor` | string | Supervisor da equipe (21 supervisores) | `supervisor_id`, `supervisor_name` |
| `cod_gerente`, `nome_gerente` | string | Gerente regional (um por região; o do NE foi trocado em 2026-01-01) | `manager_id`, `manager_name` |
| `regiao_comercial` | string | SUDESTE, SUL, NORDESTE, CENTRO-OESTE, NORTE, CONTAS NACIONAIS | `commercial_region` |
| `dt_inicio_vigencia`, `dt_fim_vigencia` | date | Vigência (intervalo fechado). Vendedor desligado: a última versão tem fim e não há versão seguinte | `valid_from`, `valid_to` |
| `dt_registro` | date | Quando a mudança foi **digitada** no ERP; pode ser depois do início da vigência | `registered_date`; `is_retroactive` = `dt_registro > dt_inicio_vigencia` (auditoria) |
| `dt_atualizacao` | timestamp | Gravação da versão | `_source_updated_at` |

**Reorganizações:** a cada virada de trimestre (2025-07-01 … 2026-10-01). Em cada uma, ~45% dos vendedores trocam de supervisor; 1 vendedor é desligado e 1 a 3 são contratados (2 por trimestre no NE, pela expansão).

| Sujeira | Quantidade | Tratamento previsto |
|---|---|---|
| Mudança cadastrada depois do início da vigência (retroativa) | 84 versões | Usar a vigência, não a data de registro (ADR-0006) |
| Vendedores do NE migrados para o SE em 2026-07-01 | 6 | SCD 2: vendas antigas continuam no NE ("como era") |
| Troca do gerente do NE em 2026-01-01 | todas as versões do NE | Gera nova versão para cada vendedor do NE |

**Caso tratado pelo pipeline:** vendedor desligado não tem versão vigente.
- **Visão "como é hoje":** as vendas dele (R$ 6,6 mi em jan–jun/2026) usam a última versão conhecida, marcada com `is_latest` (ADR-0008).
- **NF emitida depois do desligamento:** o vendedor do item de NF é resolvido na data do pedido, não na da NF. Assim, 255 itens (80 NFs) não caem no `-1` (ADR-0010).

### 3.4 `cep_municipio`: referência de CEP (fonte `ref`)

**Grão:** 1 linha por CEP. **Chave:** `cep`. **Carga:** única. **Linhas:** 5.005. Fica em `/Volumes/core/landing/ref/`, não em `erp/`: é uma referência mantida pela engenharia (no mundo real, a base de CEPs dos Correios), não um dado do ERP.

| Coluna | Tipo | Significado | Silver (`ref_postal_code`) |
|---|---|---|---|
| `cep` | string | CEP sintético, com prefixo plausível para a UF | `postal_code` |
| `municipio` | string | Nome oficial do município, com acento: a grafia "certa" | `city` |
| `uf` | string | Estado | `state` |

Sem sujeira: é a régua usada para limpar o cadastro. Cobre 49 municípios.

### 3.5 `pedidos_cab`: cabeçalho do pedido

**Grão:** 1 linha por pedido (**na fonte vêm duplicatas**). **Chave:** `num_pedido`. **Extração:** incremental. **Linhas:** 821.048 (805.153 pedidos).

| Coluna | Tipo | Significado e regra | Silver |
|---|---|---|---|
| `num_pedido` | string | Número do pedido (`PV00000001`…) | `order_id` |
| `cod_cliente` | string | Cliente que comprou | `customer_id` |
| `cod_vendedor` | string | Vendedor responsável pela carteira do cliente na data do pedido | `sales_rep_id` |
| `dt_pedido` | date | **Data de negócio** do fato de vendas (ADR-0003) | `order_date` |
| `status` | string | ABERTO, PARCIALMENTE FATURADO, FATURADO, CANCELADO | `order_status` |
| `vl_total_pedido` | decimal(15,2) | Soma dos `vl_total_item` corretos do pedido | `order_total_amount` |
| `dt_criacao` | timestamp | Quando o pedido foi digitado (horário comercial) | `created_at` |
| `dt_atualizacao` | timestamp | Última mudança (faturamento ou cancelamento). **Define qual linha é a mais recente** | `_source_updated_at` (`SEQUENCE BY`) |

| Sujeira | Quantidade | Tratamento previsto |
|---|---|---|
| Duplicata exata (reenvio da integração) | 11.994 linhas | Dedup na silver (`AUTO CDC` por `num_pedido`) |
| Quase-duplicata: mesma linha com status antigo (`ABERTO`) e `dt_atualizacao` anterior | 3.901 linhas | `AUTO CDC ... SEQUENCE BY dt_atualizacao` fica com a mais recente |
| Pedido cancelado depois de faturado | 2.200 | Fica no `fact_sales` com status `CANCELADO`; no `fact_invoice`, a NF e a devolução se anulam |
| Pedido de cliente que ainda não está no cadastro | 313 | Membro `-1` até o cadastro chegar |

### 3.6 `pedidos_itens`: itens do pedido

**Grão:** 1 linha por item de pedido. **Chave:** `num_pedido` + `item_pedido`. **Extração:** incremental. **Linhas:** 2.934.877 (~3,6 itens por pedido: PDV ~3,3, rede ~19, distribuidor ~26).

| Coluna | Tipo | Significado e regra | Silver |
|---|---|---|---|
| `num_pedido` | string | Pedido | `order_id` |
| `item_pedido` | int | Sequência do item no pedido (1, 2, …); o mesmo produto não se repete no pedido | `order_item_number` |
| `cod_produto` | string | Produto vendido (sempre ativo na data do pedido) | `product_id` |
| `quantidade` | int | Unidades de venda (caixas/packs/barris) | `quantity` |
| `vl_unitario` | decimal(15,2) | Preço por unidade de venda, já com o preço do canal | `unit_price` |
| `vl_desconto` | decimal(15,2) | Desconto da linha, em reais | `discount_amount` |
| `vl_total_item` | decimal(15,2) | `quantidade × vl_unitario − vl_desconto` | `net_amount` (+ `gross_amount` derivado) |
| `dt_atualizacao` | timestamp | Igual à criação do pedido (itens não mudam depois) | `_source_updated_at` |

| Sujeira | Quantidade | Tratamento previsto |
|---|---|---|
| Quantidade e valores negativos (erro de integração) | 3.038 itens | Expectation `drop` na silver, contada nas métricas do pipeline (devolução não é pedido negativo: vem por NF) |

### 3.7 `notas_fiscais`: cabeçalho da NF

**Grão:** 1 linha por NF. **Chave:** `num_nf` + `serie`. **Extração:** incremental. **Linhas:** 838.957 (827.799 de venda e 11.158 de devolução).

| Coluna | Tipo | Significado e regra | Silver |
|---|---|---|---|
| `num_nf` | string | Número da NF (9 dígitos, sequencial pela emissão) | `invoice_id` |
| `serie` | string | Série da NF (sempre `1`) | `invoice_series` |
| `num_pedido` | string | Pedido de origem. Na devolução, o pedido da venda devolvida | `order_id` |
| `num_nf_origem` | string | Só na devolução: a NF de venda que está sendo devolvida | `original_invoice_id` |
| `cod_cliente` | string | Cliente | `customer_id` |
| `cfop` | string | Operação fiscal: define venda × devolução (tabela da seção 2) | `cfop` → `invoice_type` |
| `dt_emissao` | date | **Data de negócio** do fato de faturamento (ADR-0003) | `issue_date` |
| `vl_total_nf` | decimal(15,2) | Soma dos itens **desta** NF (positivo também na devolução) | `invoice_total_amount` |
| `dt_atualizacao` | timestamp | Momento da emissão | `_source_updated_at` |

| Sujeira | Quantidade | Tratamento previsto |
|---|---|---|
| Devolução misturada com venda | 11.158 NFs (8.958 devoluções parciais + 2.200 por cancelamento) | `invoice_type` a partir do CFOP; sinal negativo no fato |
| Faturamento parcial: o pedido sai em 2 NFs | 39.110 pedidos (+130 aguardando a 2ª NF) | Grão no item de NF; conciliação por `order_id` + `order_item_number` |
| Total da NF ≠ total do pedido | consequência dos centavos e das parciais | Esperado: cada fato tem seu número (ADR-0003) |

### 3.8 `notas_fiscais_itens`: itens da NF

**Grão:** 1 linha por item de NF. **Chave:** `num_nf` + `serie` + `item_nf`. **Extração:** incremental. **Linhas:** 3.033.885 (25.357 de devolução).

| Coluna | Tipo | Significado e regra | Silver |
|---|---|---|---|
| `num_nf`, `serie` | string | NF | `invoice_id`, `invoice_series` |
| `item_nf` | int | Sequência do item na NF | `invoice_item_number` |
| `num_pedido`, `item_pedido` | string, int | **Item de pedido** que está sendo faturado ou devolvido | `order_id`, `order_item_number` |
| `cod_produto` | string | Produto | `product_id` |
| `cfop` | string | Mesmo CFOP do cabeçalho | `cfop` |
| `quantidade` | int | Quantidade desta NF. Na parcial, ~60% vai na 1ª NF e o resto na 2ª | `quantity` |
| `vl_unitario` | decimal(15,2) | Mesmo preço do item do pedido | `unit_price` |
| `vl_desconto` | decimal(15,2) | Desconto proporcional à quantidade desta NF | `discount_amount` |
| `vl_total_item` | decimal(15,2) | Valor do item na NF (positivo na devolução) | `net_amount` (com sinal no fato) |
| `dt_atualizacao` | timestamp | Momento da emissão | `_source_updated_at` |

| Sujeira | Quantidade | Tratamento previsto |
|---|---|---|
| Valor diverge do pedido em ±1 a 3 centavos | 90.031 itens (~3%). O gerador marcou o delta sem olhar a quantidade: 476 desses itens têm quantidade 0 e são descartados na silver, que fica com 89.555 | Esperado: o faturamento vale o fiscal; diferença visível na conciliação. No `fact_invoice`, bruto = líquido + desconto (ADR-0010) |
| Itens de devolução misturados | 25.357 | `invoice_type` pelo CFOP |
| **Não planejado:** item com quantidade 0 na 2ª NF de um faturamento parcial (defeito do gerador quando o item pedido tinha 2 unidades) | 16.285 itens, 945 NFs só com esses itens. 476 deles também receberam o delta de centavos: são os −R$ 0,61 dessas NFs; os outros 15.809 têm líquido 0 | Descartado na silver (`DROP ROW`); gerador corrigido depois desta carga (ADR-0008, conciliação no ADR-0010). A correção vale só para pedidos cuja 1ª NF sai depois de 2026-10-18: as extrações diárias de 2026-10-20 a 2026-10-22 trazem mais 71 itens assim, a 2ª NF dos pedidos parciais da carga (§5, ADR-0012 D10) |

**Integridade verificada:**
- todo item de NF aponta para um item de pedido existente;
- nenhum item foi faturado acima da quantidade pedida.

---

## 4. Verificações feitas após a geração

| Verificação | Resultado |
|---|---|
| Itens de NF sem item de pedido | 0 |
| Itens de pedido cujo vendedor não tem versão vigente na data | 0 |
| Clientes com mais de uma versão vigente | 0 |
| Versões de cliente com vigência sobreposta ou invertida | 0 |
| Quantidade faturada maior que a pedida | 0 |
| Distribuição de versões por cliente | 17.429 com 1 · 13.838 com 2 · 8.506 com 3 ou mais |
| Participação por canal (hl) | 55,0% distribuidor · 26,2% rede · 18,8% PDV (o caso pede ~55/25/20) |
| Participação por marca (hl) | Pilsen 46,8% · Puro Malte 26,4% · Guará 15,6% · Alvorada 7,3% · Névoa Zero 3,9% |

**Desvios em relação ao caso de negócio, aceitos e registrados:**
- **Portfólio:** ~80 SKUs ativos, e não ~120.
- **Hierarquia:** ~500 versões, e não ~600.
- **Região:** o SE fica com 56% do volume geográfico por causa das redes.
- **Crescimento do NE:** +77%, contra os ~35% planejados, porque carteira e frequência cresceram juntas.

---

## 5. Extração diária (desde 2026-10-20)

Depois do go-live, o job `serra_azul_daily` faz o papel do ERP. A tarefa `simulate_erp_extraction` grava na landing uma extração por dia ([ADR-0012](adr/0012-job-diario-extracao-simulada-idempotencia-e-verificacao.md)). A extração da data **E** traz o que mudou no dia **D = E − 1**.

### 5.1 O que cada extração traz

| Dataset | Extração | Conteúdo da extração E |
|---|---|---|
| `pedidos_cab` | incremental | Pedidos criados em D e cabeçalhos cujo status mudou em D (faturamento, cancelamento), com o status vigente em D |
| `pedidos_itens` | incremental | Itens dos pedidos criados em D. Itens não mudam depois |
| `notas_fiscais` | incremental | NFs emitidas em D: venda, 2ª NF do faturamento parcial e devolução |
| `notas_fiscais_itens` | incremental | Itens dessas NFs |
| `clientes_pdv` | full | Todas as versões conhecidas em E, com o histórico inteiro |
| `produtos` | full | Todas as versões, com o histórico inteiro |
| `hierarquia_comercial` | full | Todas as versões, com o histórico inteiro |
| `cep_municipio` | — | Não é reenviado: carga única |

- **Mesmo mundo da carga inicial:** as fórmulas são as mesmas (o simulador importa o gerador da carga), e a numeração continua sem buraco: pedidos a partir de `PV00805154`, NFs a partir de `100838958`.
- **Determinística:** o conteúdo de cada extração é função só da sua data.

### 5.2 Arquivos

- **Caminho:** `/Volumes/core/landing/erp/<dataset>/<E>/<dataset>.parquet`, um arquivo por dataset e por data.
- **Reenvio:** `<dataset>_reenvio.parquet`, na mesma pasta e com o mesmo conteúdo (parâmetro `resend=true`). Simula o reenvio de integração do ERP. Existe para a extração de 2026-10-27 (prova de idempotência). O sufixo é fixo por data: o próximo reenvio de teste precisa ser de outra data.
- **Nenhum arquivo é sobrescrito ou regravado,** salvo para o direito ao esquecimento (LGPD). A carga inicial (2026-10-19) continua nos arquivos `part-<uuid>` em que foi gravada.
- **PII:** cada extração de `clientes_pdv` traz a PII de todos os clientes. Por isso a landing e a bronze guardam uma cópia da PII por dia até a retenção do ADR-0007 ganhar rotina (ADR-0012).
- **Catch-up:** o simulador grava todas as extrações que faltam até a data alvo. A primeira execução, em 2026-10-27, gravou 8 de uma vez (2026-10-20 a 2026-10-27).

### 5.3 Linhas por extração

| Extração (E) | Dados de (D) | `pedidos_cab` | `pedidos_itens` | `notas_fiscais` | `notas_fiscais_itens` | `clientes_pdv` | `produtos` | `hierarquia_comercial` |
|---|---|---|---|---|---|---|---|---|
| 2026-10-20 | 2026-10-19 (seg) | 2.713 | 8.888 | 1.203 | 4.288 | 72.698 | 126 | 505 |
| 2026-10-21 | 2026-10-20 (ter) | 3.585 | 8.663 | 2.058 | 7.559 | 72.732 | 126 | 505 |
| 2026-10-22 | 2026-10-21 (qua) | 4.003 | 8.609 | 2.447 | 9.038 | 72.767 | 126 | 505 |
| 2026-10-23 | 2026-10-22 (qui) | 3.981 | 8.672 | 2.492 | 8.887 | 72.801 | 126 | 505 |
| 2026-10-24 | 2026-10-23 (sex) | 4.149 | 9.423 | 2.487 | 8.836 | 72.845 | 126 | 505 |
| 2026-10-25 | 2026-10-24 (sáb) | 2.385 | 2.571 | 1.887 | 6.823 | 72.887 | 126 | 505 |
| 2026-10-26 | 2026-10-25 (dom) | 893 | 165 | 854 | 2.978 | 72.932 | 126 | 505 |
| 2026-10-27 | 2026-10-26 (seg) | 2.760 | 9.012 | 1.239 | 4.295 | 72.976 | 126 | 505 |
| **Total** | | **24.469** | **56.003** | **14.667** | **52.704** | **582.638** | **1.008** | **4.040** |

- **Reenvio de 2026-10-27:** as mesmas contagens da linha de 2026-10-27, 90.913 linhas nos 7 datasets.
- **Sábado fraco e domingo quase zero**, como na carga (§2): o dia de pedido mais fraco é o domingo, 2026-10-25 (165 itens).
- **A bronze cresceu exatamente o total** de cada coluna na primeira execução (ADR-0012, Resultados).

### 5.4 Clientes fora da carga

- Os 227 clientes da FK órfã (§3.1) chegam no cadastro numa extração entre 2026-10-20 e 2026-10-29, sorteada pela chave do cliente: `go_live + 1 + floor(u("chegada_cadastro") × 10)` dias.
- Na extração de 2026-10-27, `clientes_pdv` tinha 39.962 clientes, contra 39.773 na carga: **189 dos 227** já tinham chegado. Os 38 restantes chegam até 2026-10-29.
- Em versões, a extração passa de 72.650 (carga) para 72.976 (2026-10-27): 326 versões dos clientes que já chegaram.
- No ensaio de 2026-10-27, 113 clientes tinham chegado até 2026-10-24, e nenhum cliente da carga sumiu.
- **Nos fatos, em 2026-10-27:** 254 itens no `fact_sales` e 256 no `fact_invoice` continuam com cliente `-1` e `_is_customer_id_known = false`, todos por cliente ainda não cadastrado. Eram 1.070 e 1.084 antes da extração diária.

### 5.5 Sujeiras

| Sujeira | Na extração diária | Tratamento |
|---|---|---|
| Duplicata exata, quase-duplicata, item negativo, centavos | Mesmo sorteio da carga, pela chave do registro. No ensaio de 2026-10-27: duplicata exata 1,55% a 2,04%; quase-duplicata ~0,3%; item negativo ~0,1%; centavos ~3%. Nas 8 extrações reais: 52 itens de pedido negativos | Os mesmos da carga (§3). Os 52 negativos foram descartados por `positive_quantity_and_amount` (event log de 2026-10-27) |
| Item de NF com quantidade 0 (2ª NF do faturamento parcial) | 71 itens (54 pedidos, todos de venda), nas extrações de 2026-10-20 a 2026-10-22. São a 2ª NF dos pedidos parciais cuja 1ª NF já estava na carga, com as 2 unidades. Depois de 2026-10-22, nenhum | Descartado na silver (`positive_quantity`, `DROP ROW`): 71 descartados no event log de 2026-10-27. A regra corrigida do gerador vale só para pedidos cuja 1ª NF sai depois de 2026-10-18 (ADR-0012 D10) |
| Reenvio da extração inteira (`_reenvio`) | Só quando o job roda com `resend=true` | Silver de fatos: `AUTO CDC`. Silver de cadastros: linhas idênticas juntadas (`GROUP BY ALL`, ADR-0012 D5) |

### 5.6 Simplificações do simulador (ADR-0012)

- **Cadastros parados depois do go-live:** nenhuma versão nova, nenhum cliente novo e nenhuma reorganização da hierarquia (a estrutura fica a de 2026-10-01). O único cadastro que chega é o dos 227 órfãos, até 2026-10-29.
- **Produtos:** o sorteio de produtos de cada mês depois do horizonte repete o último mês, sem lançamento nem descontinuação.
- **Tendência de crescimento:** depois de 2026-10-18, continua a reta do horizonte.
- **Calendário:** feriados, Natal e Carnaval até o fim de 2027. Depois disso, nenhum feriado.

### 5.7 Verificações (ensaio de 2026-10-27)

A extração de 2026-10-19 regerada pelo simulador foi comparada linha a linha (`EXCEPT ALL` nos dois sentidos) com o **recorte do dia 2026-10-18** da carga inicial, ou seja, o que uma extração diária de 2026-10-19 traria:
- `pedidos_cab`: pedidos cuja última `dt_atualizacao` é de 2026-10-18;
- `pedidos_itens`: itens dos pedidos **criados** (`dt_pedido`) em 2026-10-18;
- `notas_fiscais` e `notas_fiscais_itens`: NFs com `dt_emissao` = 2026-10-18, e os itens dessas NFs;
- cadastros: a extração full inteira.

| Verificação | Resultado |
|---|---|
| Extração de 2026-10-19 regerada pelo simulador × recorte do dia 2026-10-18 da carga | 0 diferenças nos 7 datasets |
| Quantidade faturada maior que a pedida | 0 entre 2.913.332 itens de pedido com NF de venda (carga + ensaio) |
| Numeração | Sem buraco: pedidos 805154 a 817326; NFs 100838958 a 100849644 |

---

## 6. Glossário

| Termo | Significado |
|---|---|
| **Carga inicial / dump** | Primeira extração, com todo o histórico; depois vêm as incrementais diárias |
| **Catch-up** | O simulador grava, numa execução, todas as extrações que faltam desde a última (por exemplo, depois de dias sem rodar o job) |
| **Reenvio** | A mesma extração entregue de novo pela integração, num arquivo com outro nome |
| **Extração full** | A tabela inteira a cada entrega (cadastros) |
| **Extração incremental** | Só o que foi criado ou alterado desde a última entrega (pedidos, NFs) |
| **SKU** | *Stock keeping unit*: o código comercial de uma combinação marca × líquido × embalagem × pack |
| **EAN** | Código de barras do produto |
| **CFOP** | Código fiscal que identifica a natureza da operação na NF |
| **hl (hectolitro)** | 100 litros: unidade de volume do setor de bebidas |
| **PDV** | Ponto de venda (bar, restaurante, mercado…) atendido diretamente |
| **Contas nacionais** | Equipe comercial que atende as redes varejistas nacionais |
| **Vigência** | Período em que uma versão de cadastro é válida |
| **FK órfã** | Registro que aponta para uma chave ainda inexistente na tabela de referência |
| **Quase-duplicata** | Duas linhas do mesmo registro com conteúdo diferente (versões em momentos diferentes) |
