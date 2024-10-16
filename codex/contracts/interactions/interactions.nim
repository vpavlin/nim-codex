import pkg/ethers
import ../clock
import ../marketplace
import ../market

export clock

type
  ContractInteractions* = ref object of RootObj
    clock*: OnChainClock

method start*(self: ContractInteractions) {.async, base.} =
  await self.clock.start()

method stop*(self: ContractInteractions) {.async, base.} =
  await self.clock.stop()
