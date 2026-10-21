# ADR-0009: Rejeitar documento com dígitos todos iguais e adiar o CNPJ alfanumérico até o contrato do ERP mudar

- **Status:** aceito
- **Data:** 2026-10-21
- **Fatia:** fatia 1 — Comercial
- **Decisor:** Engenheiro de Dados Sênior (Claude) · **Acompanhamento:** usuário (pode vetar)
- **Relação:** emenda o [ADR-0008](0008-pipeline-core-deduplicacao-qualidade-e-cadastros.md), regra "documento com dígito inválido"

## Contexto

O ADR-0008 valida o dígito verificador do CNPJ/CPF por módulo 11 na `silver.erp_customer` e guarda só `is_document_valid`, como aviso. Relendo o código, apareceram duas lacunas:
- **Dígitos todos iguais passam.** `000.000.000-00` e `111.111.111-11` satisfazem o módulo 11 e eram marcados como válidos. São os "documentos-coringa" que um cadastro feito às pressas usa quando o cliente não informa o CPF.
- **CNPJ alfanumérico.** Desde julho de 2026, a Receita emite CNPJ com letras nas 12 primeiras posições (IN RFB 2.229/2024). Os dois dígitos verificadores continuam numéricos, calculados por módulo 11 com cada caractere valendo o seu código ASCII − 48. A normalização atual apaga tudo o que não é dígito, então um CNPJ alfanumérico válido seria marcado como inválido.

O contrato da extração diz que `cnpj_cpf` vem "só dígitos (14 ou 11)" ([dicionário](../dicionario-de-dados-erp.md)), e os dados sintéticos não têm CNPJ alfanumérico.

## Decisão

**1. Documento com todos os dígitos iguais é inválido.**
- A regra entra no `is_document_valid`: `doc <> repeat(substr(doc, 1, 1), length(doc))`.
- Continua sendo aviso, não descarte: o cliente existe e compra (argumento do ADR-0008).
- Foi testada no warehouse com as mesmas expressões do pipeline, em 9 casos: CPF e CNPJ válidos, dígito errado, zeros, repetidos, tamanho errado e nulo. Todos deram o resultado esperado.

**2. O CNPJ alfanumérico não é implementado agora.**
- O contrato do ERP é numérico. Um documento com letras seria uma **mudança de contrato da fonte**, e o formato exato (com ou sem máscara, maiúsculas) precisa ser acordado com o time do ERP, não adivinhado.
- Se chegar antes disso, nada se perde: a linha passa, só o flag fica errado e a contagem do aviso `document_valid` sobe. Esse aumento é o sinal para revisitar.

**Efeito na carga de 2026-10-19 (execução de 2026-10-21):** nenhum. Continuam 1.336 versões (735 clientes) com documento inválido, porque os inválidos do gerador só trocam o último dígito. A regra corrige um caso que os dados sintéticos não exercitam; os 9 casos de teste são a prova de que ela funciona.

## Alternativas consideradas

| Alternativa | Por que não |
|---|---|
| Implementar o CNPJ alfanumérico já | A mudança no SQL é pequena, mas antecipa um formato que o contrato não prevê e que nenhum dado exercita. O aviso existente detecta a chegada |
| Regex com retrovisor (`^(\d)\1+$`) | Funciona, mas o escape de barra invertida em literal SQL é fonte de erro. `repeat` diz o mesmo de forma legível |
| Lista fixa de documentos proibidos | São 10 valores por tamanho de documento; a regra geral cobre todos sem manter lista |
| Passar documento inválido para `DROP ROW` | O cliente existe e compra; descartá-lo tiraria vendas reais do fato |

## Consequências

- **Positivas:**
  - o flag fica correto para documentos-coringa;
  - a regra segue como aviso, sem perder cliente;
  - é uma materialized view, então a regra nova vale para todo o histórico na próxima execução, sem *full refresh*.
- **Negativas / dívidas assumidas:**
  - um CNPJ alfanumérico real seria marcado como inválido até a mudança.
- **O que nos faria revisitar:**
  - a contagem do aviso `document_valid` subir sem explicação;
  - o time do ERP anunciar o novo formato.

## Em produção faríamos diferente?

**Sim, no CNPJ alfanumérico.** A Receita já emite desde julho de 2026, então num ERP real o suporte entraria agora, junto com a confirmação do formato com o time do ERP. A mudança:
- manter as letras na normalização, em maiúsculas;
- valor de cada caractere = `ascii(c) − 48`, que para dígitos dá o mesmo valor de hoje;
- exigir os dois dígitos verificadores numéricos.

O exemplo publicado pela Receita, `12.ABC.345/01DE-35`, serve de caso de teste.

**Em 10x e 100x de volume:** nada muda. A regra é por linha, com custo linear e desprezível.

## Conceitos para estudar

- dígito verificador: o que ele prova (digitação) e o que não prova (existência);
- documento-coringa em cadastro;
- mudança de formato por regulação como mudança de contrato de dados;
- expectation de aviso como detector de mudança na fonte.
