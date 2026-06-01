package com.kafkaprocon.kafkaapi.model;

public class InventarioEvent {

    private int    pedidoId;
    private String producto;
    private int    stockRestante;
    private String estado;        // ACTUALIZADO | SIN_STOCK

    
    
    
    public InventarioEvent() {
		
	}

	public InventarioEvent(int pedidoId, String producto,
                           int stockRestante, String estado) {
        this.pedidoId      = pedidoId;
        this.producto      = producto;
        this.stockRestante = stockRestante;
        this.estado        = estado;
    }

    public int    getPedidoId()      { return pedidoId; }
    public String getProducto()      { return producto; }
    public int    getStockRestante() { return stockRestante; }
    public String getEstado()        { return estado; }

    public void setPedidoId(int pedidoId)           { this.pedidoId = pedidoId; }
    public void setProducto(String producto)         { this.producto = producto; }
    public void setStockRestante(int stockRestante)  { this.stockRestante = stockRestante; }
    public void setEstado(String estado)             { this.estado = estado; }
}