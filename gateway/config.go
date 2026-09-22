package main

type Config struct {
	EcommerceURL string `env:"ECOMMERCE_URL,required"`
	IdentityURL  string `env:"IDENTITY_URL,required"`
	Port         int    `env:"PORT,required"`
}
