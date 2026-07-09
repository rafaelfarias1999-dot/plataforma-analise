/// Buffer circular genérico de capacidade fixa.
///
/// # Decisão de Arquitetura
/// - Zero alocação após inicialização (`Vec` preenchido com `T::default()`).
/// - Ao atingir a capacidade, o dado mais antigo é silenciosamente sobrescrito.
/// - Índices lógicos permitem acesso aos N itens mais recentes.
/// - Projetado para envolver com `Arc<RwLock>` para concorrência de múltiplos leitores.
#[derive(Debug)]
pub struct RingBuffer<T> {
    buffer: Vec<T>,
    capacity: usize,
    /// Posição do próximo write.
    write_pos: usize,
    /// Número total de itens empurrados (pode ser maior que a capacidade).
    total_pushed: usize,
}

impl<T: Clone + Default> RingBuffer<T> {
    /// Cria um novo RingBuffer com a capacidade especificada.
    ///
    /// # Zero Alocação
    /// Pré-aloca e preenche o vetor com `T::default()`. Operações subsequentes
    /// não realizam `malloc`.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "A capacidade do RingBuffer deve ser maior que 0");
        Self {
            buffer: vec![T::default(); capacity],
            capacity,
            write_pos: 0,
            total_pushed: 0,
        }
    }

    /// Empurra um item para o buffer.
    ///
    /// Se o buffer estiver cheio, o item mais antigo será sobrescrito
    /// silenciosamente (comportamento circular esperado para streaming).
    pub fn push(&mut self, item: T) {
        self.buffer[self.write_pos] = item;
        self.write_pos = (self.write_pos + 1) % self.capacity;
        self.total_pushed += 1;
    }

    /// Retorna o número de itens válidos no buffer.
    pub fn len(&self) -> usize {
        self.total_pushed.min(self.capacity)
    }

    /// Retorna a capacidade máxima do buffer.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Verifica se o buffer atingiu sua capacidade.
    pub fn is_full(&self) -> bool {
        self.total_pushed >= self.capacity
    }

    /// Verifica se o buffer está vazio.
    pub fn is_empty(&self) -> bool {
        self.total_pushed == 0
    }

    /// Limpa o buffer.
    pub fn clear(&mut self) {
        self.write_pos = 0;
        self.total_pushed = 0;
    }

    /// Retorna o item no índice lógico fornecido.
    ///
    /// `0` é o item mais recente. `1` é o segundo mais recente, etc.
    /// Retorna `None` se o índice estiver fora dos limites ou não houver dados.
    pub fn get(&self, logical_idx: usize) -> Option<&T> {
        let len = self.len();
        if logical_idx >= len {
            return None;
        }

        // Se write_pos for 0, o índice lógico 0 está na posição capacity - 1
        let mut physical_idx = self.write_pos as isize - 1 - logical_idx as isize;
        if physical_idx < 0 {
            physical_idx += self.capacity as isize;
        }

        Some(&self.buffer[physical_idx as usize])
    }

    /// Retorna um snapshot dos últimos `count` itens.
    /// O primeiro item retornado é o mais antigo do snapshot.
    pub fn snapshot(&self, count: usize) -> Vec<T> {
        let count = count.min(self.len());
        if count == 0 {
            return Vec::new();
        }

        let mut res = Vec::with_capacity(count);
        // Itera de count-1 (mais antigo) até 0 (mais recente)
        for i in (0..count).rev() {
            if let Some(item) = self.get(i) {
                res.push(item.clone());
            }
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_and_get() {
        let mut rb = RingBuffer::<i32>::new(3);
        assert!(rb.is_empty());
        
        rb.push(10);
        assert_eq!(rb.len(), 1);
        assert_eq!(rb.get(0), Some(&10));
        
        rb.push(20);
        assert_eq!(rb.get(0), Some(&20));
        assert_eq!(rb.get(1), Some(&10));
        
        rb.push(30); // buffer cheio
        assert_eq!(rb.get(0), Some(&30));
        assert_eq!(rb.get(1), Some(&20));
        assert_eq!(rb.get(2), Some(&10));
        assert_eq!(rb.get(3), None);
        assert!(rb.is_full());

        rb.push(40); // wrap around
        assert_eq!(rb.get(0), Some(&40));
        assert_eq!(rb.get(1), Some(&30));
        assert_eq!(rb.get(2), Some(&20));
        assert_eq!(rb.get(3), None);
        assert_eq!(rb.len(), 3);
    }

    #[test]
    fn test_snapshot() {
        let mut rb = RingBuffer::<i32>::new(5);
        rb.push(1);
        rb.push(2);
        rb.push(3);
        
        let snap = rb.snapshot(2);
        assert_eq!(snap, vec![2, 3]);
        
        let snap_all = rb.snapshot(10);
        assert_eq!(snap_all, vec![1, 2, 3]);
    }
}
