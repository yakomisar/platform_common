package transaction

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"

	"github.com/yakomisar/chat/internal/client/db"
	"github.com/yakomisar/chat/internal/client/db/pg"
)

type manager struct {
	db db.Transactor
}

// NewTransactionManager создает новый менеджер транзакций,
// который удовлетворяет интерфейсу db.TxManager
func NewTransactionManager(transactor db.Transactor) db.TxManager {
	return &manager{
		db: transactor,
	}
}

// transaction основная функция, которая выполняет
// указанный пользователем обработчик в транзакции
func (m *manager) transaction(ctx context.Context, opts pgx.TxOptions, fn db.Handler) (err error) {
	// Если это вложенная транзакция, пропускаем
	// инициацию новой транзакции и выполняем обработчик.
	tx, ok := ctx.Value(pg.TxKey).(pgx.Tx)
	if ok {
		return fn(ctx)
	}

	// Стартуем новую транзакцию
	tx, err = m.db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("can't begin transaction: %v", err)
	}

	// Кладем транзакцию в контекст
	ctx = pg.MakeContextTx(ctx, tx)

	// Настраиваем функцию отсрочки для отката
	// или коммита транзакциио
	defer func() {
		// восстанавливаемся после паники
		if r := recover(); r != nil {
			err = fmt.Errorf("panic recovered: %v", r)
		}

		// откатываем транзакцию, если произошла ошибка
		if err != nil {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
				err = fmt.Errorf("rollback error: %v; original error: %w", rollbackErr, err)
			}
			return
		}

		// если ошибок не было, коммитим транзакцию
		if commitErr := tx.Commit(ctx); commitErr != nil {
			err = fmt.Errorf("commit error: %v", commitErr)
		}
	}()

	// Выполним код внутри транзакции.
	// если функция терпит неудачу, возвращаем ошибку,
	// и функция отсрочки выполняет откат
	// или в противном случае транзакция коммитится.
	if err = fn(ctx); err != nil {
		err = fmt.Errorf("failed executing code inside transaction: %v", err)
	}

	return err
}

func (m *manager) ReadCommitted(ctx context.Context, f db.Handler) error {
	txOpts := pgx.TxOptions{IsoLevel: pgx.ReadCommitted}
	return m.transaction(ctx, txOpts, f)
}
