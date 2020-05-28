package usecase

import (
	"gitlab.com/thorchain/midgard/pkg/clients/thorchain"
)

var _ thorchain.Thorchain = (*ThorchainDummy)(nil)

// ThorchainDummy is test purpose implementation of Thorchain.
type ThorchainDummy struct{}

func (t *ThorchainDummy) GetNodeAccounts() ([]thorchain.NodeAccount, error) {
	return nil, ErrNotImplemented
}

func (t *ThorchainDummy) GetVaultData() (thorchain.VaultData, error) {
	return thorchain.VaultData{}, ErrNotImplemented
}

func (t *ThorchainDummy) GetConstants() (thorchain.ConstantValues, error) {
	return thorchain.ConstantValues{}, nil
}

func (t *ThorchainDummy) GetAsgardVaults() ([]thorchain.Vault, error) {
	return nil, ErrNotImplemented
}

func (t *ThorchainDummy) GetLastChainHeight() (thorchain.LastHeights, error) {
	return thorchain.LastHeights{}, ErrNotImplemented
}
