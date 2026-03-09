-- =============================================================
-- CDC Full Stack - Init Script PostgreSQL
-- Tabelas: pedidos, emprestimos, emprestimos_aprovados
-- =============================================================

-- Habilita extensão para UUIDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================
-- TABELA: emprestimos (todas as solicitações)
-- =============================================================
CREATE TABLE IF NOT EXISTS public.emprestimos (
    id                  SERIAL PRIMARY KEY,
    uuid                UUID DEFAULT uuid_generate_v4() NOT NULL,
    cliente_nome        VARCHAR(100) NOT NULL,
    cliente_cpf         VARCHAR(14) NOT NULL,
    cliente_email       VARCHAR(150) NOT NULL,
    valor_solicitado    NUMERIC(12, 2) NOT NULL,
    taxa_juros_proposta NUMERIC(5, 2),
    parcelas_solicitadas INT NOT NULL,
    finalidade          VARCHAR(100) NOT NULL,
    status              VARCHAR(20) NOT NULL DEFAULT 'pendente'
                            CHECK (status IN ('pendente', 'em_analise', 'aprovado', 'reprovado', 'cancelado')),
    motivo_reprovacao   VARCHAR(255),
    solicitado_em       TIMESTAMP DEFAULT NOW(),
    atualizado_em       TIMESTAMP DEFAULT NOW()
);

-- =============================================================
-- TABELA: emprestimos_aprovados (apenas aprovados - via trigger)
-- =============================================================
CREATE TABLE IF NOT EXISTS public.emprestimos_aprovados (
    id                  SERIAL PRIMARY KEY,
    emprestimo_id       INT NOT NULL REFERENCES public.emprestimos(id),
    uuid                UUID DEFAULT uuid_generate_v4() NOT NULL,
    cliente_nome        VARCHAR(100) NOT NULL,
    cliente_cpf         VARCHAR(14) NOT NULL,
    cliente_email       VARCHAR(150) NOT NULL,
    valor_solicitado    NUMERIC(12, 2) NOT NULL,
    valor_aprovado      NUMERIC(12, 2) NOT NULL,
    taxa_juros          NUMERIC(5, 2) NOT NULL,
    parcelas            INT NOT NULL,
    valor_parcela       NUMERIC(10, 2) NOT NULL,
    finalidade          VARCHAR(100) NOT NULL,
    aprovado_em         TIMESTAMP DEFAULT NOW(),
    vencimento_em       TIMESTAMP,
    atualizado_em       TIMESTAMP DEFAULT NOW()
);

-- =============================================================
-- FUNÇÃO: atualiza campo atualizado_em automaticamente
-- =============================================================
CREATE OR REPLACE FUNCTION update_atualizado_em()
RETURNS TRIGGER AS $$
BEGIN
    NEW.atualizado_em = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER trg_emprestimos_atualizado_em
    BEFORE UPDATE ON public.emprestimos
    FOR EACH ROW EXECUTE FUNCTION update_atualizado_em();

CREATE TRIGGER trg_emprestimos_aprovados_atualizado_em
    BEFORE UPDATE ON public.emprestimos_aprovados
    FOR EACH ROW EXECUTE FUNCTION update_atualizado_em();

-- =============================================================
-- FUNÇÃO + TRIGGER: ao aprovar emprestimo, copia para aprovados
-- =============================================================
CREATE OR REPLACE FUNCTION fn_aprovar_emprestimo()
RETURNS TRIGGER AS $$
DECLARE
    v_taxa          NUMERIC(5,2);
    v_valor_aprov   NUMERIC(12,2);
    v_parcela       NUMERIC(10,2);
BEGIN
    -- Só age quando status muda PARA 'aprovado'
    IF NEW.status = 'aprovado' AND OLD.status <> 'aprovado' THEN

        -- Define taxa baseada no valor solicitado
        v_taxa := CASE
            WHEN NEW.valor_solicitado <= 5000   THEN 3.49
            WHEN NEW.valor_solicitado <= 15000  THEN 2.49
            WHEN NEW.valor_solicitado <= 30000  THEN 1.99
            ELSE 1.59
        END;

        -- Valor aprovado pode ser até 90% do solicitado (simulação de análise)
        v_valor_aprov := ROUND(NEW.valor_solicitado * 0.95, 2);

        -- Calcula parcela simples (sem juros compostos para simplificar)
        v_parcela := ROUND(v_valor_aprov / NEW.parcelas_solicitadas, 2);

        INSERT INTO public.emprestimos_aprovados (
            emprestimo_id,
            cliente_nome,
            cliente_cpf,
            cliente_email,
            valor_solicitado,
            valor_aprovado,
            taxa_juros,
            parcelas,
            valor_parcela,
            finalidade,
            aprovado_em,
            vencimento_em
        ) VALUES (
            NEW.id,
            NEW.cliente_nome,
            NEW.cliente_cpf,
            NEW.cliente_email,
            NEW.valor_solicitado,
            v_valor_aprov,
            v_taxa,
            NEW.parcelas_solicitadas,
            v_parcela,
            NEW.finalidade,
            NOW(),
            NOW() + (NEW.parcelas_solicitadas || ' months')::INTERVAL
        );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_emprestimo_aprovado
    AFTER UPDATE ON public.emprestimos
    FOR EACH ROW EXECUTE FUNCTION fn_aprovar_emprestimo();

-- =============================================================
-- REPLICA IDENTITY para CDC (Debezium capturar todos os campos)
-- =============================================================
ALTER TABLE public.emprestimos REPLICA IDENTITY FULL;
ALTER TABLE public.emprestimos_aprovados REPLICA IDENTITY FULL;
