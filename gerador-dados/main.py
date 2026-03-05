"""
CDC Full Stack - Gerador contínuo de empréstimos
Execute: python seed_emprestimos.py
Ctrl+C para parar.
"""

import time
import random
import logging

import psycopg2
from faker import Faker

# =============================================================
# CONFIG
# =============================================================
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "emprestimos_db",
    "user":     "cdc_user",
    "password": "cdc_pass",
}

FINALIDADES = [
    "Reforma residencial",
    "Compra de veiculo",
    "Capital de giro",
    "Educacao",
    "Viagem internacional",
    "Emergencia medica",
    "Consolidacao de dividas",
    "Expansao de negocio",
    "Compra de equipamentos",
    "Aquisicao de imovel",
    "Casamento",
    "Tratamento de saude",
]

MOTIVOS_REPROVACAO = [
    "Score de credito insuficiente",
    "Renda incompativel com o valor solicitado",
    "Restricoes no CPF",
    "Historico de inadimplencia",
    "Documentacao incompleta",
    "Capacidade de pagamento insuficiente",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

fake = Faker("pt_BR")


# =============================================================
# HELPERS
# =============================================================
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def gerar_emprestimo() -> dict:
    valor = round(random.uniform(1000, 80000), 2)
    return {
        "cliente_nome":         fake.name(),
        "cliente_cpf":          fake.cpf(),
        "cliente_email":        fake.email(),
        "valor_solicitado":     valor,
        "taxa_juros_proposta":  round(random.uniform(1.49, 3.99), 2),
        "parcelas_solicitadas": random.choice([6, 9, 12, 18, 24, 36, 48]),
        "finalidade":           random.choice(FINALIDADES),
    }


def inserir(conn, dados: dict) -> int:
    sql = """
        INSERT INTO public.emprestimos
            (cliente_nome, cliente_cpf, cliente_email,
             valor_solicitado, taxa_juros_proposta,
             parcelas_solicitadas, finalidade, status)
        VALUES
            (%(cliente_nome)s, %(cliente_cpf)s, %(cliente_email)s,
             %(valor_solicitado)s, %(taxa_juros_proposta)s,
             %(parcelas_solicitadas)s, %(finalidade)s, 'pendente')
        RETURNING id
    """
    with conn.cursor() as cur:
        cur.execute(sql, dados)
        emp_id = cur.fetchone()[0]
    conn.commit()
    return emp_id


def atualizar_status(conn, emp_id: int, status: str, motivo: str = None):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE public.emprestimos SET status = %s, motivo_reprovacao = %s WHERE id = %s",
            (status, motivo, emp_id)
        )
    conn.commit()


def sortear_status_final() -> tuple:
    status = random.choices(
        ["aprovado", "reprovado", "cancelado", "em_analise"],
        weights=[0.55, 0.25, 0.10, 0.10],
        k=1
    )[0]
    motivo = random.choice(MOTIVOS_REPROVACAO) if status == "reprovado" else None
    return status, motivo


EMOJI = {
    "aprovado":   "✅",
    "reprovado":  "❌",
    "cancelado":  "⚠️",
    "em_analise": "🔄",
}


# =============================================================
# LOOP PRINCIPAL
# =============================================================
def main():
    log.info("Iniciando gerador de empréstimos... (Ctrl+C para parar)")
    conn = get_connection()
    total = 0

    try:
        while True:
            # 1. Insere solicitação
            dados = gerar_emprestimo()
            emp_id = inserir(conn, dados)
            total += 1
            log.info(f"[#{total}] ID {emp_id} | {dados['cliente_nome']} | R$ {dados['valor_solicitado']:,.2f} | {dados['parcelas_solicitadas']}x | {dados['finalidade']}")

            # 2. Simula tempo de análise (aleatório entre 1s e 5s)
            tempo_analise = random.uniform(1, 5)
            time.sleep(tempo_analise)

            # 3. Muda para em_analise
            atualizar_status(conn, emp_id, "em_analise")
            log.info(f"  └─ ID {emp_id} → 🔄 em_analise ({tempo_analise:.1f}s)")

            # 4. Simula decisão (aleatório entre 2s e 8s)
            tempo_decisao = random.uniform(2, 8)
            time.sleep(tempo_decisao)

            # 5. Status final (trigger cuida de emprestimos_aprovados se aprovado)
            status_final, motivo = sortear_status_final()
            atualizar_status(conn, emp_id, status_final, motivo)
            log.info(f"  └─ ID {emp_id} → {EMOJI[status_final]} {status_final}" +
                     (f" | {motivo}" if motivo else ""))

            # 6. Pausa aleatória antes do próximo (entre 0.5s e 4s)
            pausa = random.uniform(0.5, 4)
            time.sleep(pausa)

    except KeyboardInterrupt:
        log.info(f"\\nEncerrado pelo usuário. Total processado: {total}")
    except Exception as e:
        log.error(f"Erro: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()