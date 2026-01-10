package relay

import (
	"context"
	"fmt"
	"time"
)

func (srv *Server) runRecoverer(ctx context.Context) {
	defer srv.wg.Done()
	ticker := time.NewTicker(recoverInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, q := range srv.queueNames() {
				n, err := srv.rdb.Recover(ctx, q)
				if err != nil && ctx.Err() == nil {
					srv.logger.Warn(fmt.Sprintf("relay: recover failed for queue %s: %v", q, err))
					continue
				}
				if n > 0 {
					srv.logger.Info(fmt.Sprintf("relay: recovered %d task(s) from queue %s", n, q))
				}
			}
		}
	}
}
