from peewee import chunked
from astra.utils import flatten
from astra.models.base import database
from astra.models.spectrum import Spectrum


class ProgressContext:
    """
    Wrapper for multiprocessing queue that supports hierarchical progress reporting.

    This class provides a clean API for migration functions to report progress,
    including support for nested subtasks that appear indented in the CLI.

    Usage:
        def my_migration(queue=None):
            if queue is None:
                queue = ProgressContext()  # No-op mode for interactive use

            # Simple progress updates
            queue.update(description="Processing", total=100)
            for i in range(100):
                queue.update(advance=1)

            # Or use subtasks for multi-phase operations
            with queue.subtask("Phase 1", total=50) as phase1:
                for i in range(50):
                    phase1.update(advance=1)

            with queue.subtask("Phase 2", total=50) as phase2:
                for i in range(50):
                    phase2.update(advance=1)
    """

    def __init__(self, queue=None, task_id=None, _parent=None):
        """
        Create a new ProgressContext.

        :param queue:
            A multiprocessing.Queue to send progress updates to. If None,
            operates in no-op mode (useful for interactive shell usage).
        :param task_id:
            Internal task identifier for hierarchy tracking. Users should not
            set this directly.
        """
        self._queue = queue
        self._task_id = task_id
        self._parent = _parent
        self._subtask_counter = 0
        self._active_subtask = None

    @property
    def is_active(self):
        """Returns True if this context is connected to an actual queue."""
        return self._queue is not None

    def update(self, description=None, total=None, completed=None, advance=None, **kwargs):
        """
        Update progress for the current task.

        :param description: New description text
        :param total: Total number of items
        :param completed: Number of items completed (absolute)
        :param advance: Number of items to advance by (relative)
        """
        if not self.is_active:
            return

        msg = {}
        if description is not None:
            msg["description"] = description
        if total is not None:
            msg["total"] = total
        if completed is not None:
            msg["completed"] = completed
        if advance is not None:
            msg["advance"] = advance
        msg.update(kwargs)

        if msg:
            self._queue.put(("update", self._task_id, msg))

    def subtask(self, description, total=None):
        """
        Create a subtask context for nested progress reporting.

        :param description: Description of the subtask
        :param total: Total number of items in this subtask
        :returns: A SubtaskContext that can be used as a context manager

        Usage:
            with queue.subtask("Loading data", total=1000) as sub:
                for item in items:
                    sub.update(advance=1)
        """
        subtask_id = f"{self._task_id}.{self._subtask_counter}" if self._task_id else str(self._subtask_counter)
        self._subtask_counter += 1
        return SubtaskContext(self, subtask_id, description, total)

    def put(self, msg):
        """
        Backward-compatible interface for raw queue.put() calls.

        Supports:
            - queue.put(dict(description="...", total=...))
            - queue.put(dict(advance=...))
            - queue.put(Ellipsis)  # Signal task completion
        """
        if not self.is_active:
            return

        if msg is Ellipsis:
            self._queue.put(Ellipsis)
        elif isinstance(msg, dict):
            self.update(**msg)
        else:
            # Pass through any other message types
            self._queue.put(msg)


class SubtaskContext:
    """
    Context manager for subtask progress reporting.

    Created by ProgressContext.subtask(), this provides progress tracking
    for a nested operation that appears indented in the CLI display.
    """

    def __init__(self, parent, task_id, description, total):
        self._parent = parent
        self._task_id = task_id
        self._description = description
        self._total = total
        self._completed = False

    @property
    def is_active(self):
        return self._parent.is_active

    def __enter__(self):
        if self.is_active:
            self._parent._queue.put((
                "add_subtask",
                self._task_id,
                self._parent._task_id,
                {"description": self._description, "total": self._total}
            ))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.is_active and not self._completed:
            self._parent._queue.put(("complete_subtask", self._task_id))
        return False

    def update(self, description=None, total=None, completed=None, advance=None, **kwargs):
        """Update progress for this subtask."""
        if not self.is_active:
            return

        msg = {}
        if description is not None:
            msg["description"] = description
        if total is not None:
            msg["total"] = total
        if completed is not None:
            msg["completed"] = completed
        if advance is not None:
            msg["advance"] = advance
        msg.update(kwargs)

        if msg:
            self._parent._queue.put(("update", self._task_id, msg))

    def complete(self):
        """Explicitly mark this subtask as complete."""
        if self.is_active and not self._completed:
            self._parent._queue.put(("complete_subtask", self._task_id))
            self._completed = True


# Backward compatibility alias
NoQueue = ProgressContext

def generate_new_spectrum_pks(N, batch_size=100):
    with database.atomic():
        # Need to chunk this to avoid SQLite limits.
        with tqdm(desc="Assigning spectrum identifiers", unit="spectra", total=N) as pb:
            for chunk in chunked([{"spectrum_flags": 0}] * N, batch_size):                
                yield from flatten(
                    Spectrum
                    .insert_many(chunk)
                    .returning(Spectrum.pk)
                    .tuples()
                    .execute()
                )
                pb.update(min(batch_size, len(chunk)))
                pb.refresh()


def enumerate_new_spectrum_pks(iter, batch_size=100):
    N = len(iter)

    with database.atomic():
        for chunk, batch in zip(chunked(iter, batch_size), chunked([{"spectrum_flags": 0}] * len(iter), batch_size)):
            spectrum_pks = flatten(
                Spectrum
                .insert_many(batch)
                .returning(Spectrum.pk)
                .tuples()
                .execute()
            )
            for spectrum_pk, item in zip(spectrum_pks, chunk):
                yield (spectrum_pk, item)


def upsert_many(model, returning, data, batch_size, queue, description):
    """
    Upsert many records with progress reporting.

    :param model: The peewee model to insert into
    :param returning: The field to return (usually pk)
    :param data: List of dicts to insert
    :param batch_size: Number of records per batch
    :param queue: ProgressContext for progress reporting
    :param description: Description for progress display
    """
    if queue is None:
        queue = ProgressContext()

    returned = []
    with database.atomic():
        with queue.subtask(description, total=len(data)) as progress:
            for chunk in chunked(data, batch_size):
                returned.extend(
                    flatten(
                        model
                        .insert_many(chunk)
                        .on_conflict_ignore()
                        .returning(returning)
                        .tuples()
                        .execute()
                    )
                )
                progress.update(advance=len(chunk))

    return tuple(returned)