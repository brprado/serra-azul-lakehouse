# ADR-0003: Modelar pedido e faturamento como dois fatos transacionais no grão do item

- **Status:** aceito — emendado pelo [ADR-0006](0006-dimensoes-conformadas-identidade-scd-e-chaves.md) (chaves das dimensões, medidas com `volume_hl`, `order_item_number` no `fact_invoice`, fatos como materialized views) e pelo [ADR-0010](0010-pipeline-commercial-fatos-chaves-e-membro-desconhecido.md) (bruto do `fact_invoice` = líquido + desconto; `invoice_series` no grão do `fact_invoice`; CFOP sem regra fica com medidas nulas, fora da soma "já líquida")
- **Data:** 2026-10-16
- **Fatia:** fatia 1 — Comercial
- **Decisor:** Engenheiro de Dados Sênior · **Revisor:** Staff Engineer

## Contexto

Dois stakeholders fazem perguntas diferentes sobre "vendas":

- **Diretor Comercial:** "quanto vendi ontem, por região, marca e canal?" — demanda capturada.
- **Controladoria:** faturamento, preço médio e desconto por canal, **batendo com o fiscal**.

O ERP entrega pedidos (cabeçalho + itens) e notas fiscais. Pedido e faturamento acontecem em datas diferentes, um pedido pode ser faturado parcialmente em várias notas, pode ser cancelado depois de faturado, e os valores da nota divergem do pedido em centavos. Notas de devolução chegam misturadas com notas de venda. A marca é atributo do produto, que só aparece no nível de item.

## Decisão

**Dois fatos transacionais, um por processo de negócio, ambos no grão mais atômico disponível (o item):**

| Fato | Grão | Fonte | Quem lê | Data de negócio |
|---|---|---|---|---|
| `commercial.gold.fact_sales` | item do pedido | `erp_pedidos_cab` + `erp_pedidos_itens` (cabeçalho desnormalizado no item) | Diretor Comercial | data do pedido |
| `commercial.gold.fact_invoice` | item da nota fiscal | `erp_notas_fiscais` + `erp_notas_fiscais_itens` | Controladoria | data de emissão da NF |

Regras:

- **Dimensões degeneradas:** `order_id` no `fact_sales`; `invoice_id` e `order_id` no `fact_invoice` (permite conciliar pedido × faturado e prepara a fatia 4).
- **Tempo do evento vs processamento:** métricas são agregadas pela data de negócio. A data de processamento fica como coluna técnica (`_updated_at`), usada só para auditoria e carga incremental.
- **Devoluções:** permanecem no `fact_invoice`, identificadas por `invoice_type` (venda / devolução) e com valores negativos; a soma já é o faturamento líquido.
- **Cancelamentos:** o item cancelado permanece no `fact_sales` com `order_status`; o dashboard comercial filtra os cancelados, preservando o histórico.
- **Mudança na fonte:** a engenharia solicitou ao "time do ERP" a inclusão de `erp_notas_fiscais_itens` (registrada no caso de negócio, seção 5.1), pois a NF em nível de cabeçalho não permitiria faturamento por SKU/marca.

## Alternativas consideradas

| Alternativa | Por que não |
|---|---|
| Fato no grão do pedido (cabeçalho) | Não responde "vendas por marca": um pedido mistura SKUs de marcas diferentes, e a marca vive no item |
| Um fato só, uma linha por pedido com coluna "faturado sim/não" (*accumulating snapshot*) | Não representa faturamento parcial (um pedido em várias NFs), obriga a escolher uma única data para "ontem", não guarda o valor fiscal da NF (diverge do pedido) e "desfaz" números já publicados quando um pedido é cancelado após faturado |
| Fato de pedido (cabeçalho) separado do fato de itens | O que o cabeçalho mediria (nº de pedidos) sai de `COUNT(DISTINCT order_id)` no fato de itens; o cabeçalho foi desnormalizado no item |
| `fact_invoice` no grão da nota | Mesma limitação do grão de pedido: sem SKU, sem faturamento por marca |

## Consequências

- Positivas: cada stakeholder tem seu número com sua definição explícita; grão atômico permite qualquer agregação (marca, SKU, região, canal); conciliação pedido × faturado possível pelo `order_id`.
- Negativas / dívidas assumidas: duas definições de "venda" convivem — o dashboard precisa deixar claro qual número é qual; atributos do cabeçalho se repetem em cada item (mais armazenamento); medidas que só existam no cabeçalho (ex.: frete ou desconto do pedido inteiro) precisarão de regra de rateio se aparecerem.
- O que nos faria revisitar: surgir medida de cabeçalho relevante sem rateio razoável; a necessidade de acompanhar o ciclo pedido → faturamento → entrega numa linha só (aí um *accumulating snapshot* complementar faria sentido — candidato na fatia 4).

## Em produção faríamos diferente?

Nada de essencial — a modelagem se sustenta em produção. Em escala, os dois fatos seriam os maiores do lakehouse: liquid clustering pela data de negócio e por chaves de filtro frequentes (região/canal) entraria na fatia 4 (tema de performance). A mudança na fonte (`erp_notas_fiscais_itens`) seria negociada com o time do ERP como um contrato de dados, com prazo e versão.

Em 10x de volume: os fatos crescem linearmente; o risco passa a ser o custo de reprocessar históricos longos, o que reforça a carga incremental pela coluna técnica de processamento.

## Conceitos para estudar

Grão (declarar o mais atômico); fato transacional vs *periodic snapshot* vs *accumulating snapshot*; cabeçalho/itens e desnormalização; dimensão degenerada; tempo do evento vs tempo de processamento; fatos aditivos e valores com sinal (devoluções); conformidade de fatos para *drill-across* (pedido × faturado).
