
export class CustomError extends Error {
    constructor(message) {
        super();
        this.message = message;
        this.stack = (new Error()).stack;
        this.name = (this.constructor as any).name;
    }
}
