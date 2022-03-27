package gscp

import (
	"context"
	"fmt"

	"github.com/skillian/expr/errors"
	"github.com/skillian/interactivity"
)

// Main is the main entry point for the program after the command line
// is parsed.
func Main(
	ctx context.Context, fromIndex bool, sourceString string,
	toIndex bool, destString string, allowOverwrite, unsecure bool,
) error {
	sourceInfo := specInfo{
		str:      sourceString,
		index:    fromIndex,
		unsecure: unsecure,
	}
	destInfo := specInfo{
		str:      destString,
		index:    toIndex,
		unsecure: unsecure,
	}
	ctx, asker := ensureAskerInContext(ctx)
	source, err := parseSpecInfoAndEnsurePassword(ctx, sourceInfo, "source", asker)
	if err != nil {
		return err
	}
	dest, err := parseSpecInfoAndEnsurePassword(ctx, destInfo, "dest", asker)
	if err != nil {
		return err
	}
	return CopyFromSourceToDestSpec(ctx, source, dest, allowOverwrite)
}

type specInfo struct {
	str      string
	index    bool
	unsecure bool
}

func parseSpecInfo(si specInfo) (*Spec, error) {
	sp, err := ParseSpec(si.str)
	if err != nil {
		return nil, err
	}
	if si.index {
		sp.Kind |= IndexSpec
	}
	if si.unsecure {
		sp.Kind |= UnsecureSpec
	}
	return sp, nil
}

func parseSpecInfoAndEnsurePassword(ctx context.Context, si specInfo, specDesc string, asker interactivity.Asker) (*Spec, error) {
	sp, err := parseSpecInfo(si)
	if err != nil {
		return nil, err
	}
	if err = ensureSpecPassword(ctx, asker, sp, specDesc); err != nil {
		return nil, err
	}
	return sp, nil
}

func ensureAskerInContext(ctx context.Context) (context.Context, interactivity.Asker) {
	asker, ok := interactivity.AskerFromContext(ctx)
	if !ok {
		asker = interactivity.ConfirmAsker(
			interactivity.AskerFunc(interactivity.ConsoleAsker),
		)
		ctx = interactivity.AddAskerToContext(ctx, asker)
	}
	return ctx, asker
}

func ensureSpecPassword(ctx context.Context, asker interactivity.Asker, sp *Spec, specDesc string) (err error) {
	if sp.IsLocal() || sp.Password != "" {
		return nil
	}
	sp.Password, err = interactivity.Ask(
		ctx, asker,
		fmt.Sprintf(
			"Password for %s username %s: ",
			specDesc, sp.Username,
		),
		interactivity.IsSecret(true),
	)
	if err != nil {
		return errors.Errorf2From(
			err, "failed to get %s %v password",
			specDesc, sp,
		)
	}
	return nil
}
