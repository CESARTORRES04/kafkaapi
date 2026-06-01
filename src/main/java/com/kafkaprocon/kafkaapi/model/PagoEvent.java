package com.kafkaprocon.kafkaapi.model;

public class PagoEvent {
    private int pedidoId;
    private String estado;   // APROBADO | RECHAZADO
    private double monto;
    private String producto;

    
    
    public PagoEvent() {
		
	}
	public PagoEvent(int pedidoId, String estado, double monto, String producto) {
        this.pedidoId = pedidoId;
        this.estado   = estado;
        this.monto    = monto;
        this.producto = producto;
    }
    // getters/setters...

	public int getPedidoId() {
		return pedidoId;
	}

	public void setPedidoId(int pedidoId) {
		this.pedidoId = pedidoId;
	}

	public String getEstado() {
		return estado;
	}

	public void setEstado(String estado) {
		this.estado = estado;
	}

	public double getMonto() {
		return monto;
	}

	public void setMonto(double monto) {
		this.monto = monto;
	}

	public String getProducto() {
		return producto;
	}

	public void setProducto(String producto) {
		this.producto = producto;
	}
    
}
