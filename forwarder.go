package relay

import (
	"context"
	"fmt"
	"time"
)

func (srv *Server) runForwarder(ctx context.Context) {
	defer srv.wg.Done()
	ticker := time.NewTicker(forwardInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, q := range srv.queueNames() {
				if _, err := srv.rdb.ForwardDue(ctx, q); err != nil && ctx.Err() == nil {
					srv.logger.Warn(fmt.Sprintf("relay: forward failed for queue %s: %v", q, err))
				}
			}
		}
	}
}
