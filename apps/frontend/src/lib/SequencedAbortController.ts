export class SequencedAbortController {
    private seq = 0;
    private controller: AbortController | null = null;

    begin(): { seq: number; signal: AbortSignal } {
        this.seq += 1;
        this.controller?.abort();
        this.controller = new AbortController();
        return {
            seq: this.seq,
            signal: this.controller.signal,
        };
    }

    isCurrent(seq: number): boolean {
        return seq === this.seq;
    }

    complete(seq: number): boolean {
        if (!this.isCurrent(seq)) {
            return false;
        }
        this.controller = null;
        return true;
    }

    cancelAll(): void {
        this.seq += 1;
        this.controller?.abort();
        this.controller = null;
    }

    currentSeq(): number {
        return this.seq;
    }
}
