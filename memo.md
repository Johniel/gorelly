# TODO

- [ ] xxxIdからxxxIDに各種命名を変える。
- [ ] BufferPoolManager.FetchPageが命名や引数に反してPageを返さない。
- [ ] `err !=` という記法をerrors.Isに変える。
- [ ] BTree.FetchRootPageが命名に反してPageを返さない。
- [ ] `it.slotId < leafNode.NumPairs()` この比較はIDの範囲に何らかの前提を要するもので筋が悪いと思う。
- [ ] `it.slotID++` 同上。 それに比してPageIDには `NextPageId()` がある。
- [ ] IsDirtyの更新がBufferの中に閉じていない
- [ ] memcmpableってbtree以下である必要ある？
