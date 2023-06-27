package auth

import (
	"fmt"

	"github.com/casbin/casbin/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func New(model, policy string) (*Authorizer, error) {
	enforcer, err := casbin.NewEnforcer(model, policy)
	if err != nil {
		return nil, fmt.Errorf("failed to create new Authorizer: %s", err)
	}
	return &Authorizer{
		enforcer: enforcer,
	}, nil
}

type Authorizer struct {
	enforcer *casbin.Enforcer
}

func (a *Authorizer) Authorize(subject, object, action string) error {
	isAuthorized, err := a.enforcer.Enforce(subject, object, action)
	if err != nil {
		return err
	}
	if !isAuthorized {
		msg := fmt.Sprintf(
			"%s not permitted to %s to %s",
			subject,
			action,
			object,
		)
		st := status.New(codes.PermissionDenied, msg)
		return st.Err()
	}
	return nil
}
