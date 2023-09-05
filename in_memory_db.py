class InMemoryDB:
    def __init__(self):
        self.data = {}
        self.transaction_stack = []
        self.value_counts = {}
        self.keys_for_value = {}

    def _remove_key_from_value_mappings(self, key, value):
        if value in self.value_counts:
            self.value_counts[value] -= 1
            if not self.value_counts[value]:
                del self.value_counts[value]

        if value in self.keys_for_value:
            self.keys_for_value[value].remove(key)
            if not self.keys_for_value[value]:
                del self.keys_for_value[value]

    def _add_key_to_value_mappings(self, key, value):
        self.value_counts[value] = self.value_counts.get(value, 0) + 1
        if value not in self.keys_for_value:
            self.keys_for_value[value] = set()
        self.keys_for_value[value].add(key)

    def set(self, key, value):
        if self.transaction_stack:
            if key not in self.transaction_stack[-1]:
                self.transaction_stack[-1][key] = self.data.get(key, None)
            old_value = self.data.get(key)
            if old_value:
                self._remove_key_from_value_mappings(key, old_value)
        else:
            old_value = self.data.get(key)
            if old_value:
                self._remove_key_from_value_mappings(key, old_value)
            self._add_key_to_value_mappings(key, value)

        self.data[key] = value

    def get(self, key):
        return self.data.get(key, "NULL")

    def unset(self, key):
        if key not in self.data:
            return

        value = self.data[key]
        if self.transaction_stack:
            if key not in self.transaction_stack[-1]:
                self.transaction_stack[-1][key] = value
            self._remove_key_from_value_mappings(key, value)
        else:
            self._remove_key_from_value_mappings(key, value)

        del self.data[key]

    def counts(self, value):
        return self.value_counts.get(value, 0)

    def find(self, value):
        return list(self.keys_for_value.get(value, []))

    def begin(self):
        self.transaction_stack.append({})

    def rollback(self):
        if not self.transaction_stack:
            return "NO TRANSACTION"

        changes = self.transaction_stack.pop()
        for key, value in changes.items():
            if value is None:
                old_value = self.data.get(key)
                if old_value:
                    self._remove_key_from_value_mappings(key, old_value)
                if key in self.data:
                    del self.data[key]
            else:
                self.data[key] = value
                self._add_key_to_value_mappings(key, value)
        return "ROLLBACK DONE"

    def commit(self):
        if not self.transaction_stack:
            return "NO TRANSACTION"
        self.transaction_stack.pop()
        return "COMMIT DONE"

    @staticmethod
    def end():
        return "ENDING SESSION"


def interactive_session():
    db = InMemoryDB()
    COMMANDS = {
        "SET": lambda _args: db.set(_args[0], _args[1]) if len(_args) == 2 else "INVALID ARGUMENTS",
        "GET": lambda _args: db.get(_args[0]) if len(_args) == 1 else "INVALID ARGUMENTS",
        "UNSET": lambda _args: db.unset(_args[0]) if len(_args) == 1 else "INVALID ARGUMENTS",
        "COUNTS": lambda _args: db.counts(_args[0]) if len(_args) == 1 else "INVALID ARGUMENTS",
        "FIND": lambda _args: " ".join(db.find(_args[0])) if len(_args) == 1 else "INVALID ARGUMENTS",
        "BEGIN": lambda _args: db.begin(),
        "ROLLBACK": lambda _args: db.rollback(),
        "COMMIT": lambda _args: db.commit(),
        "END": lambda _args: db.end()
    }

    while True:
        try:
            cmd_input = input("> ")
            if not cmd_input:
                continue
            cmd, *args = cmd_input.split()
            if cmd in COMMANDS:
                result = COMMANDS[cmd](args)
                if result:
                    print(result)
            else:
                print("INVALID COMMAND")
        except EOFError:
            print("\nENDING SESSION")
            break


interactive_session()
