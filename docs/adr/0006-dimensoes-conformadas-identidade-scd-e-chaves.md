# ADR-0006: Modelar as dimensões conformadas da fatia 1 com identidade estável, SCD por atributo e chaves substitutas determinísticas

- **Status:** aceito — emendado pelo [ADR-0008](0008-pipeline-core-deduplicacao-qualidade-e-cadastros.md) (§5: `is_latest` no lugar de `is_current` na visão "como é hoje"), pelo [ADR-0010](0010-pipeline-commercial-fatos-chaves-e-membro-desconhecido.md) (chave resolvida na data do documento que carrega a FK: vendedor do `fact_invoice` na data do pedido, cliente na da NF; motivo do `-1` exposto nos fatos; `dim_product` pela última versão de cada produto, e não pela versão atual; entrega a regra de vigências sobrepostas prometida nas Consequências) e pelo [ADR-0011](0011-dim-customer-tipo-1-pela-ultima-versao.md) (§2: atributos tipo 1 da `dim_customer` pela última versão de cada cliente, e não pela versão corrente)
- **Data:** 2026-10-19
- **Fatia:** fatia 1 — Comercial
- **Decisor:** Engenheiro de Dados Sênior (Claude) · **Acompanhamento:** usuário (pode vetar)
- **Relação:** emenda o ADR-0003 (chaves e medidas dos fatos) e substitui o uso de `AUTO CDC ... SCD TYPE 2` previsto no ADR-0002 para as dimensões.

## Contexto

Os fatos do ADR-0003 precisam de cliente, produto, vendedor e data. O ERP entrega os três primeiros em **extração full diária, já com o histórico de versões e a data de vigência de cada uma**.

As sujeiras que afetam a modelagem são:
- SKU descontinuado e reaproveitado para outro produto;
- cliente que muda de canal e de endereço;
- grafias diferentes para a mesma cidade;
- reorganização comercial trimestral, às vezes cadastrada dias depois de entrar em vigor.

O Diretor quer vendas por região, marca e canal, comparadas com a meta. O critério de pronto da fatia exige responder "quem era o supervisor do vendedor X em tal data".

Esta decisão fecha a discussão de dimensões iniciada em 2026-10-19. As escolhas do usuário mantidas estão indicadas abaixo.

## Decisão

| Dimensão | Grão | Identidade (chave durável) | Histórico | Tipo de SCD |
|---|---|---|---|---|
| `dim_date` | 1 dia | `date` | nenhum | 0: gerada, de 2024-01-01 a 2030-12-31 |
| `dim_product` | 1 produto | `product_id`, o id interno do ERP, nunca reaproveitado | não | 1 |
| `dim_customer` | 1 versão de cliente | `customer_id` | SCD 2 em canal, tipo de cliente, endereço, cidade, UF e região geográfica; tipo 1 em razão social e status | 2, por atributo |
| `dim_sales_rep` | 1 versão de vendedor | `sales_rep_id` | SCD 2 em supervisor, gerente e região comercial; tipo 1 no nome | 2 |

Todas ficam em `core.gold`, como materialized views.

### 1. Identidade vem antes de histórico

O SKU reaproveitado é um problema de **identidade**, não de versão. A solução é a que o usuário escolheu (opção B): o ERP fornece um `product_id` que nunca é reaproveitado, e o SKU passa a ser um atributo.

Resolvida a identidade, o produto **não precisa de SCD 2**:
- descrição e EAN alterados são correções;
- reclassificação de categoria é lida "como é hoje", para comparar períodos sob a mesma árvore de categorias.

### 2. O histórico vem da vigência do ERP, não da comparação de extrações

Como cada extração full traz o histórico completo de versões, a dimensão é recalculada a cada execução a partir da **extração mais recente**:
- versões consecutivas sem mudança nos atributos rastreados são fundidas, para que só esses atributos gerem versão;
- os atributos de tipo 1 vêm da versão corrente.

Isso traz duas vantagens:
- cadastro retroativo (registrado no dia 10, com vigência a partir do dia 1) fica com a data certa;
- os 18 meses de histórico existem desde a primeira carga.

### 3. Vigência em intervalo semiaberto

O intervalo é `[valid_from, valid_to)`:
- `valid_to` é o `valid_from` da versão seguinte;
- a versão corrente tem `valid_to = 9999-12-31` e `is_current = true`.

A junção com o fato é `event_date >= valid_from AND event_date < valid_to`. Com `BETWEEN`, que fecha as duas pontas, a venda do dia da troca casaria com duas versões e seria duplicada. Com `valid_to` nulo, as vendas da versão atual se perderiam.

### 4. Chaves substitutas determinísticas

- **Cliente e vendedor:** `<entidade>_key = xxhash64(<entidade>_id, valid_from)`.
- **Produto:** `xxhash64(product_id)`.
- **Membro "desconhecido":** cada dimensão tem uma linha com chave `-1`.

Como a chave é calculada a partir dos dados, ela tem o mesmo valor em toda recomputação ou *full refresh*. Uma coluna `IDENTITY` renumeraria as linhas e quebraria os fatos.

### 5. Os fatos guardam duas chaves por dimensão

- `<entidade>_key` aponta para a versão vigente na data do evento: é a visão **"como era"**, o padrão.
- `<entidade>_id` é a chave durável: juntando com `is_current`, dá a visão **"como é hoje"**.

As chaves são resolvidas **no pipeline**, não no dashboard. Assim, o warehouse 2X-Small não paga um *range join* a cada consulta, e nenhum consumidor reimplementa a junção por vigência.

### 6. FK órfã (dimensão que chega atrasada)

O fato não descarta a linha: ela aponta para `-1`. Como os fatos são materialized views recalculadas a cada execução, a linha se corrige sozinha quando o cadastro chega. Uma expectation `warn` conta as órfãs a cada execução.

### 7. A "região" do dashboard é a região comercial do vendedor, como era na data da venda

As metas são definidas para a estrutura comercial. As contas nacionais (redes, 25% do volume) são atendidas por um time que não segue a geografia.

A região geográfica do cliente fica disponível como filtro secundário: a expansão no Nordeste é uma pergunta de mercado.

No caso do João, as vendas de janeiro ficam no Nordeste. A visão "como é hoje" está disponível pelo `sales_rep_id`.

### 8. Cidade padronizada na silver

A cidade é padronizada por uma tabela de referência CEP → município, sintética e gerada junto com os dados (escolha do usuário). A padronização acontece **antes** do cálculo de versões, para que uma correção de grafia nunca vire versão nova.

### 9. Emenda ao ADR-0003: medidas e ligações dos fatos

**Medidas:**
- `quantity`: unidades de venda, em SKU;
- `volume_hl`: hectolitros, calculado como `quantity × litros por SKU / 100`. É a métrica de volume da indústria de bebidas;
- `gross_amount`, `discount_amount` e `net_amount`, em `DECIMAL(18,2)` e BRL.

**Ligações:**
- o `fact_invoice` ganha `order_item_number`, para conciliar o faturamento parcial item a item;
- os fatos são materialized views em `commercial.gold`.

### 10. A tabela de preço sai da fatia 1

O desconto praticado vem do item da NF. A `erp_tabela_preco` entra quando a análise contra o preço de lista for pedida.

**Mudanças nos dados sintéticos** (registradas no caso de negócio):
- produto com `product_id`;
- versões de cliente e produto com `valid_from`;
- `sales_rep_id` no cabeçalho do pedido;
- item de NF com `order_id` e `order_item_number`;
- nova referência CEP → município.

## Alternativas consideradas

| Alternativa | Por que não |
|---|---|
| SCD 2 do produto pelo SKU, com o produto novo como versão | A visão "como é hoje" atribuiria vendas da Pilsen à Névoa Zero, e a série histórica por SKU misturaria dois produtos (problema sério na fatia 5) |
| Regra no pipeline para detectar "produto novo" (mudança de marca ou categoria) | Heurística erra em *rebranding*. Identidade é responsabilidade da fonte e se resolve por contrato |
| `AUTO CDC ... SCD TYPE 2` ou `AUTO CDC FROM SNAPSHOT`, previstos no ADR-0002 | A sequência seria a data da extração: o histórico começaria no go-live e o cadastro retroativo ficaria com a data errada. São a ferramenta certa quando a fonte **não** guarda histórico. O `AUTO CDC` continua previsto para deduplicar os fatos na silver (SCD 1) |
| Chave natural no fato e junção por vigência no dashboard | O custo se repetiria a cada consulta no 2X-Small, e cada consumidor teria de reimplementar o join, inclusive o erro do `BETWEEN` |
| Coluna `IDENTITY` como chave substituta | Renumera no *full refresh*, e os fatos ficariam apontando para as versões erradas |
| SCD 2 em todos os atributos do cliente | Telefone e grafia gerariam versões sem valor analítico, além de levar PII para a gold |
| Região geográfica como região do dashboard | Não bate com as metas nem com o atendimento das contas nacionais |
| `date_key` inteiro `AAAAMMDD` | É herança de bancos relacionais. No Delta, o tipo `DATE` é compacto, legível e dispensa conversões |

## Consequências

- **Positivas:**
  - "como era" e "como é hoje" estão disponíveis sem lógica no BI;
  - o histórico é completo desde o primeiro dia;
  - recalcular é idempotente;
  - órfãs se resolvem sozinhas;
  - o critério de pronto ("supervisor do vendedor X em tal data") é uma consulta simples à `dim_sales_rep`.
- **Negativas / dívidas assumidas:**
  - os fatos são recalculados a cada execução (~3 mi de linhas, barato hoje);
  - se o ERP reescrever uma versão passada, um mês já reportado pode mudar. Fica aceito e registrado;
  - a qualidade da vigência passa a depender do ERP. Uma expectation detecta vigências sobrepostas.
- **O que nos faria revisitar:**
  - a fonte deixar de mandar o histórico, o que levaria a `AUTO CDC FROM SNAPSHOT`;
  - o negócio exigir relatórios fechados que não podem mudar, o que levaria a um *snapshot* mensal congelado;
  - o tempo de recálculo dos fatos pesar na janela.

## Em produção faríamos diferente?

- **Recálculo dos fatos:** materialized view com *refresh* incremental (o SDP decide quando consegue fazê-lo), ou uma tabela incremental com `MERGE` que reprocessa só as linhas com chave `-1`.
- **Relatórios fechados:** um *snapshot* mensal "fechado" para a contabilidade.
- **Contrato:** o `product_id` e a vigência fariam parte de um contrato de dados formal com o time do ERP.

**Em 10x de volume** (~30 mi de itens): a materialized view continua viável no serverless.

**Em 100x:** o *refresh* incremental passa a ser obrigatório, junto com *liquid clustering* por data (tema da fatia 4).

## Conceitos para estudar

- identidade vs versão;
- chave natural, chave durável e chave substituta;
- SCD 0, 1 e 2, e SCD por atributo (híbridos de Kimball, tipos 6 e 7);
- intervalos semiabertos;
- *late-arriving dimension* e membro desconhecido;
- vigência vs data de registro (bitemporalidade);
- hash determinístico vs `IDENTITY`;
- medidas aditivas e hectolitro como métrica do setor.
