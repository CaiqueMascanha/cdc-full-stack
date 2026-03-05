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
-- DADOS FAKE: emprestimos (15 solicitacoes - varios status)
-- =============================================================
INSERT INTO public.emprestimos (cliente_nome, cliente_cpf, cliente_email, valor_solicitado, taxa_juros_proposta, parcelas_solicitadas, finalidade, status, motivo_reprovacao) VALUES
('Ana Silva',        '123.456.789-00', 'ana.silva@email.com',       15000.00, 1.99, 24, 'Reforma residencial',    'pendente',   NULL),
('Bruno Costa',      '234.567.890-11', 'bruno.costa@email.com',      8000.00, 2.49, 12, 'Compra de veiculo',      'em_analise', NULL),
('Carla Mendes',     '345.678.901-22', 'carla.mendes@email.com',    25000.00, 1.79, 36, 'Capital de giro',        'em_analise', NULL),
('Diego Rocha',      '456.789.012-33', 'diego.rocha@email.com',      5000.00, 2.99,  6, 'Viagem',                 'reprovado',  'Score de credito insuficiente'),
('Elisa Ferreira',   '567.890.123-44', 'elisa.ferreira@email.com',  50000.00, 1.59, 48, 'Aquisicao de imovel',    'pendente',   NULL),
('Felipe Alves',     '678.901.234-55', 'felipe.alves@email.com',    12000.00, 2.19, 18, 'Educacao',               'em_analise', NULL),
('Gabriela Lima',    '789.012.345-66', 'gabriela.lima@email.com',    3000.00, 3.49,  6, 'Emergencia medica',      'reprovado',  'Renda incompativel com o valor'),
('Henrique Souza',   '890.123.456-77', 'henrique.souza@email.com',  18000.00, 1.89, 30, 'Reforma comercial',      'pendente',   NULL),
('Isabela Nunes',    '901.234.567-88', 'isabela.nunes@email.com',    9000.00, 2.29, 12, 'Compra de equipamentos', 'em_analise', NULL),
('Joao Pereira',     '012.345.678-99', 'joao.pereira@email.com',    30000.00, 1.69, 48, 'Expansao de negocio',    'pendente',   NULL),
('Karen Oliveira',   '111.222.333-44', 'karen.oliveira@email.com',   6000.00, 2.79,  9, 'Consolidacao de dividas','cancelado',  NULL),
('Lucas Martins',    '222.333.444-55', 'lucas.martins@email.com',   20000.00, 1.99, 36, 'Reforma residencial',    'em_analise', NULL),
('Marina Castro',    '333.444.555-66', 'marina.castro@email.com',    4500.00, 3.19,  6, 'Viagem internacional',   'pendente',   NULL),
('Nicolas Barbosa',  '444.555.666-77', 'nicolas.barbosa@email.com', 11000.00, 2.09, 24, 'Compra de veiculo',      'em_analise', NULL),
('Olivia Santos',    '555.666.777-88', 'olivia.santos@email.com',   35000.00, 1.75, 48, 'Aquisicao de imovel',    'pendente',   NULL);

-- =============================================================
-- APROVANDO alguns emprestimos (trigger popula emprestimos_aprovados)
-- =============================================================
UPDATE public.emprestimos SET status = 'aprovado' WHERE cliente_cpf IN (
    '123.456.789-00',
    '345.678.901-22',
    '678.901.234-55',
    '890.123.456-77',
    '222.333.444-55',
    '555.666.777-88'
);

-- =============================================================
-- REPLICA IDENTITY para CDC (Debezium capturar todos os campos)
-- =============================================================
ALTER TABLE public.emprestimos REPLICA IDENTITY FULL;
ALTER TABLE public.emprestimos_aprovados REPLICA IDENTITY FULL;
