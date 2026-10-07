//  SPDX-FileCopyrightText: 2026 Nextcloud GmbH and Nextcloud contributors
//  SPDX-License-Identifier: LGPL-3.0-or-later

@preconcurrency import FileProvider
import Foundation

///
/// Containers a `notify_push` message named as changed, waiting to be scanned.
///
/// ## Why
///
/// A push already tells us exactly what changed, within a second or so. The extension used to treat
/// it as a bare doorbell — `processFileIdsChanged` checked whether *any* of the ids were locally
/// known, threw the ids away, and signalled the whole working set. That turned a one-file change
/// into a walk of every materialised item: one measured pass took 305 seconds to surface a single
/// 31-byte file the push had identified 1.5 seconds after it was created.
///
/// Recording the ids instead lets the working-set derivation read just those containers. A depth-1
/// read of the parent is what reveals a new child, a modified child and a removed child alike, so
/// one narrow read covers every case the wide walk did — for the items the push actually named.
///
/// ## Why the full scan stays
///
/// Push is not a guarantee. Messages are lost while the socket is down, and the server only
/// propagates etags up the tree — a push tells us a subtree changed, not that nothing else did.
/// Targeted scans are therefore an accelerator layered over the full walk, never a replacement:
/// ``shouldRunFullScan(now:)`` forces one before the first walk, when one was requested with
/// ``requestFullScan(at:)`` because the push connection was just re-established or a failure needs
/// the server's current state, and once ``fullScanInterval`` has elapsed, so anything push missed is
/// still reconciled. A derivation with nothing targeted and no walk due does not read the server.
///
/// Guarded by an `NSLock` and process-wide, matching ``PendingMaterializationRegistry``. One
/// extension process serves one domain.
///
final class RemoteChangeTargets: @unchecked Sendable {
    static let shared = RemoteChangeTargets()

    ///
    /// How long a targeted-only run may go before a full reconciliation is forced anyway.
    ///
    /// A backstop, not the main defence: the window in which pushes are lost, a dropped connection,
    /// already ends with a requested full walk.
    ///
    static let fullScanInterval: TimeInterval = 60 * 60

    private let lock = NSLock()
    private var pending = Set<NSFileProviderItemIdentifier>()
    private var lastFullScan: Date?
    private var fullScanRequestedAt: Date?

    ///
    /// Note that a push named these containers as changed.
    ///
    /// Callers pass containers, not the changed items themselves: for a file that is its parent
    /// directory, whose depth-1 read shows the file's new state or its absence.
    ///
    func record(containers: some Sequence<NSFileProviderItemIdentifier>) {
        lock.lock()
        defer { lock.unlock() }
        pending.formUnion(containers)
    }

    ///
    /// Take the containers accumulated since the last derivation, clearing them.
    ///
    /// Returns `nil` when nothing is pending, which the caller reads as "no container to re-read".
    ///
    func consumeTargets() -> Set<NSFileProviderItemIdentifier>? {
        lock.lock()
        defer { lock.unlock() }

        guard !pending.isEmpty else { return nil }

        let targets = pending
        pending.removeAll()
        return targets
    }

    ///
    /// The containers accumulated so far, without clearing them.
    ///
    /// A scan already in flight uses this to pick up containers a push named after it started, so a
    /// change does not have to wait for the walk to end. The targets stay pending deliberately: the
    /// derivation that consumes them decides between a targeted and a full walk on whether anything
    /// is pending, so clearing them here would make the next pass a needless full reconciliation.
    /// The cost is that the next targeted pass re-reads a container this one already covered, which
    /// is one PROPFIND.
    ///
    func peekTargets() -> Set<NSFileProviderItemIdentifier> {
        lock.lock()
        defer { lock.unlock() }
        return pending
    }

    ///
    /// Whether this derivation must be a full walk rather than a targeted one.
    ///
    /// True until the first full scan has run, while a requested one is outstanding, and again once
    /// ``fullScanInterval`` has elapsed since the last — so a long stream of pushes can never
    /// postpone reconciliation indefinitely.
    ///
    func shouldRunFullScan(now: Date = Date()) -> Bool {
        lock.lock()
        defer { lock.unlock() }

        guard fullScanRequestedAt == nil, let lastFullScan else { return true }

        return now.timeIntervalSince(lastFullScan) >= Self.fullScanInterval
    }

    ///
    /// Demand a full walk even when pushes have targeted containers, for a signal that names no
    /// container: the main app's notification without file ids, sent whenever the push connection
    /// has been (re-)established, or a failure only the server's current state can settle.
    ///
    func requestFullScan(at date: Date = Date()) {
        lock.lock()
        defer { lock.unlock() }
        fullScanRequestedAt = date
    }

    ///
    /// Record that a full walk which started at `startedAt` completed, restarting the interval.
    ///
    /// A request made after the walk started stays outstanding, because the walk may already have
    /// read the folders that changed before that request.
    ///
    func noteFullScanCompleted(startedAt: Date) {
        lock.lock()
        defer { lock.unlock() }
        lastFullScan = startedAt

        if let fullScanRequestedAt, fullScanRequestedAt <= startedAt {
            self.fullScanRequestedAt = nil
        }
    }

    /// Drop all state. Test seam — the registry is a process-wide singleton.
    func removeAll() {
        lock.lock()
        defer { lock.unlock() }
        pending.removeAll()
        lastFullScan = nil
        fullScanRequestedAt = nil
    }
}
