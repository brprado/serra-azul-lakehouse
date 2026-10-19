# ADR-0004: Ajustar a organização do lakehouse — nomes por entidade, prefixo de fonte até a silver, landing por dataset e PII por minimização

- **Status:** aceito
- **Data:** 2026-10-19
- **Fatia:** fatia 1 (revisão da fase 0)
- **Decisor:** Engenheiro de Dados Sênior (Claude) · **Acompanhamento:** usuário (pode vetar)
- **Relação:** emenda o ADR-0001. A estrutura de catálogos e schemas, a propriedade de fatos e dimensões, os grants por catálogo e as tags continuam valendo.

## Contexto

Na revisão de 2026-10-19, o ADR-0001 foi relido contra o que a fatia 1 já exige. A estrutura se sustenta: `core` alinhado à fonte até a silver, catálogos de domínio a partir da gold. Quatro pontos, porém, falhariam assim que a primeira tabela fosse criada:

- os nomes das dimensões descrevem estrutura, não o grão;
- a regra de nomes da silver não foi definida;
- o layout da landing mistura datasets diferentes na mesma pasta;
- a PII "mascarada da silver em diante" depende de recurso cuja disponibilidade no Free Edition não foi verificada.

Mudar agora custa zero, porque nenhuma tabela existe ainda.

## Decisão

**Mantém-se a estrutura do ADR-0001, com cinco ajustes.**

**1. Dimensões nomeadas pela entidade que formam o grão**

| Antes | Agora | Por quê |
|---|---|---|
| `dim_pos` | `dim_customer` | O cliente de um pedido também pode ser distribuidor (12) ou rede varejista (2), que somam ~80% do volume. "PDV" descreve só o canal direto. |
| `dim_sales_hierarchy` | `dim_sales_rep` | O grão é o vendedor; supervisor, gerente e região comercial são atributos dele. Um nome de estrutura esconde o grão. |

Fica corrigido também o exemplo `fact_orders` do ADR-0001: os fatos são `fact_sales` e `fact_invoice` (ADR-0003).

**2. Prefixo de fonte na bronze e também na silver**

As duas camadas são alinhadas à fonte. Sem o prefixo, `core.silver.delivery` não diria se o dado veio do TMS ou do ERP, e duas fontes com a mesma entidade colidiriam.

As tabelas usam o nome da entidade em inglês, no singular: `erp_order_header`, `erp_order_item`, `erp_invoice_header`, `erp_invoice_item`, `erp_customer`, `erp_product`, `erp_sales_hierarchy`. O mapeamento completo entre fonte e tabela está em [convencoes.md](../convencoes.md).

**3. Convenção de colunas** (não existia):
- `<entidade>_id` é a chave natural e durável vinda da fonte;
- `<entidade>_key` é a chave substituta (`BIGINT`);
- `*_date` é `DATE`;
- `*_at` é `TIMESTAMP`;
- `*_amount` é `DECIMAL(18,2)` em BRL;
- `is_*` e `has_*` são booleanos;
- `valid_from` / `valid_to` / `is_current` registram a vigência;
- colunas técnicas começam com `_` (`_source_file`, `_ingested_at`, `_extraction_date`, `_updated_at`).

**4. Landing organizada por dataset e depois por data**

O layout passa a ser `/Volumes/core/landing/<fonte>/<dataset>/<AAAA-MM-DD>/<arquivo>`, e não mais `<fonte>/<data>/...`.
- **Motivo:** cada streaming table da bronze lê, com Auto Loader, um diretório com um único schema. Com a data primeiro, todos os datasets do dia ficam na mesma pasta, e cada leitura precisaria filtrar arquivos por nome.
- **Nome do dataset:** é o que a fonte usa (`pedidos_cab`), porque a landing guarda o dado como ele chega. A tradução para o inglês acontece na bronze.
- **Formato:** os extratos do ERP chegam em **Parquet**, que é tipado e compacto. A sujeira de formato (CSV/XLSX, encoding, datas em três formatos) fica no sell-out da fatia 3, onde ela é o objeto de estudo.

**5. PII por minimização, não por mascaramento**

- **Regra:** as colunas com PII (CPF/CNPJ, nome do responsável, e-mail, telefone) **não saem da bronze**. A silver projeta só as colunas de que a análise precisa.
- **Validações:** o que precisa do documento roda na passagem para a silver e vira flag. Exemplo: a validade do CNPJ vira `is_document_valid`.
- **Acesso:** a bronze tem acesso restrito, e as colunas com PII levam a tag `pii = true`, para inventário.

## Alternativas consideradas

| Alternativa | Por que não |
|---|---|
| Manter `dim_pos` e `dim_sales_hierarchy` | Os nomes não dizem o grão, e os distribuidores ficariam classificados como "PDV" |
| Silver sem prefixo de fonte | Colisão entre fontes e perda de rastreabilidade na camada mais consultada pelos engenheiros |
| Landing `<fonte>/<data>/<arquivo>` (original) | Exigiria filtro por nome de arquivo em cada leitura, e reprocessar um dataset obrigaria a varrer todos os outros |
| Mascarar PII na silver com *column mask*/ABAC | A PII continuaria copiada em mais camadas, já que máscara controla a leitura e não reduz o que é guardado. Além disso, depende de recurso não verificado no Free Edition |
| Guardar hash do CPF na silver | Sem segredo, o hash é reversível por força bruta: há só ~10⁹ CPFs válidos. Com segredo, é preciso gerir a chave. Nenhuma análise da fatia precisa do documento, então não vale o custo |

## Consequências

- **Positivas:**
  - os nomes revelam o grão;
  - as camadas alinhadas à fonte ficam rastreáveis;
  - a ingestão fica simples, com um diretório por tabela;
  - não há PII para esquecer de mascarar;
  - o direito ao esquecimento se resume a apagar na bronze e na landing (ADR-0007);
  - nada depende de recurso não verificado.
- **Negativas / dívidas assumidas:**
  - um uso futuro que precise de contato (por exemplo, uma campanha) não encontra o dado na silver. Será preciso pedir acesso à bronze, com finalidade justificada;
  - quem leu o ADR-0001 precisa saber que os nomes mudaram (registrado em [convencoes.md](../convencoes.md)).
- **O que nos faria revisitar:**
  - uma demanda recorrente por PII fora da bronze;
  - a confirmação de que *column masks* estão disponíveis, o que permitiria uma view mascarada sobre a bronze para usos específicos.

## Em produção faríamos diferente?

**PII:** o princípio seria o mesmo, a necessidade (LGPD, art. 6º, III). Com ABAC e *column masks* disponíveis, finalidades que precisam de PII, como CRM ou cobrança, ganhariam views mascaradas sobre a bronze, com acesso auditado. A PII continuaria fora da silver.

**Nomes e layout:** não mudariam com o volume. Em 10x e 100x a decisão se sustenta.

## Conceitos para estudar

- grão e nome de dimensão;
- dados alinhados à fonte vs alinhados ao consumo;
- layout de landing e Auto Loader (um schema por diretório);
- minimização, mascaramento, pseudonimização e anonimização (e por que hash de CPF não anonimiza);
- tags de dados para inventário LGPD.
